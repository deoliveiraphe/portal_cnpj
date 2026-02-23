"""
Management command para carga ETL dos arquivos ZIP de CNPJ no PostgreSQL.

Usa psycopg2 copy_expert para máxima performance de inserção.

Uso:
    python manage.py load_cnpj --competencia 2025-06
    python manage.py load_cnpj --all
    python manage.py load_cnpj --competencia 2025-06 --replace
"""
import io
import logging
import zipfile
from datetime import date, datetime
from django.utils import timezone
from pathlib import Path

import pandas as pd
import psycopg2
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import connection
from tqdm import tqdm

from cnpj.models import CargaLog

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# LAYOUTS (colunas conforme Leiaute RF CNPJ Aberta)
# ─────────────────────────────────────────────

COLUNAS = {
    "empresa": [
        "cnpj_basico",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "porte",
        "ente_federativo_responsavel",
    ],
    "estabelecimento": [
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "identificador_matriz_filial",
        "nome_fantasia",
        "situacao_cadastral",
        "data_situacao_cadastral",
        "motivo_situacao_cadastral",
        "nome_cidade_exterior",
        "pais",
        "data_inicio_atividade",
        "cnae_fiscal_principal",
        "cnae_fiscal_secundaria",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "uf",
        "municipio",
        "ddd1",
        "telefone1",
        "ddd2",
        "telefone2",
        "ddd_fax",
        "fax",
        "correio_eletronico",
        "situacao_especial",
        "data_situacao_especial",
    ],
    "socio": [
        "cnpj_basico",
        "identificador_socio",
        "nome_socio",
        "cnpj_cpf_socio",
        "qualificacao_socio",
        "data_entrada_sociedade",
        "pais",
        "representante_legal",
        "nome_representante",
        "qualificacao_representante",
        "faixa_etaria",
    ],
    "simples": [
        "cnpj_basico",
        "opcao_simples",
        "data_opcao_simples",
        "data_exclusao_simples",
        "opcao_mei",
        "data_opcao_mei",
        "data_exclusao_mei",
    ],
    "cnae": ["codigo", "descricao"],
    "municipio": ["codigo", "descricao"],
    "pais": ["codigo", "descricao"],
    "natureza": ["codigo", "descricao"],
    "qualificacao": ["codigo", "descricao"],
    "motivo": ["codigo", "descricao"],
}

# Mapeamento: prefixo do arquivo → tipo de tabela
ARQUIVO_TIPO = {
    "Empresas": "empresa",
    "Estabelecimentos": "estabelecimento",
    "Socios": "socio",
    "Simples": "simples",
    "Cnaes": "cnae",
    "Municipios": "municipio",
    "Paises": "pais",
    "Naturezas": "natureza",
    "Qualificacoes": "qualificacao",
    "Motivos": "motivo",
}

# Tabelas de domínio (sem campo competencia)
TABELAS_DOMINIO = {"cnae", "municipio", "pais", "natureza", "qualificacao", "motivo"}

# Colunas de data que precisam de parse
COLUNAS_DATA = {
    "empresa": [],
    "estabelecimento": ["data_situacao_cadastral", "data_inicio_atividade", "data_situacao_especial"],
    "socio": ["data_entrada_sociedade"],
    "simples": ["data_opcao_simples", "data_exclusao_simples", "data_opcao_mei", "data_exclusao_mei"],
    "cnae": [],
    "municipio": [],
    "pais": [],
    "natureza": [],
    "qualificacao": [],
    "motivo": [],
}

# Nome das tabelas no banco
DB_TABELA = {
    "empresa": "cnpj_empresa",
    "estabelecimento": "cnpj_estabelecimento",
    "socio": "cnpj_socio",
    "simples": "cnpj_simples",
    "cnae": "cnpj_cnae",
    "municipio": "cnpj_municipio",
    "pais": "cnpj_pais",
    "natureza": "cnpj_natureza",
    "qualificacao": "cnpj_qualificacao",
    "motivo": "cnpj_motivo",
}

CHUNK_SIZE = 100_000


# ─────────────────────────────────────────────
# FUNÇÕES DE TRANSFORMAÇÃO
# ─────────────────────────────────────────────

def _parse_data(valor: str) -> str | None:
    """Converte YYYYMMDD string para formato ISO date ou None."""
    if not valor or valor.strip() in ("", "0", "00000000"):
        return None
    v = valor.strip()
    try:
        d = datetime.strptime(v, "%Y%m%d").date()
        return d.isoformat()
    except ValueError:
        return None


def _mascarar_cpf(cpf: str) -> str:
    """
    Mascara CPF de sócio: oculta os 3 primeiros dígitos e os 2 verificadores.
    Formato: ***456789**
    """
    if not cpf or len(cpf.strip()) != 11:
        return cpf
    c = cpf.strip()
    return f"***{c[3:9]}**"


def _normalizar_string(valor) -> str | None:
    """Strip, upper, vazio → None."""
    if pd.isna(valor) or valor is None:
        return None
    s = str(valor).strip().upper()
    return s if s else None


def _transformar_chunk(df: pd.DataFrame, tipo: str, competencia: str | None) -> pd.DataFrame:
    """Aplica todas as transformações no chunk."""
    # Normalizar strings
    for col in df.columns:
        if col not in COLUNAS_DATA.get(tipo, []):
            df[col] = df[col].apply(_normalizar_string)

    # Mascarar CPF de sócios (identificador_socio == '1' é PF)
    if tipo == "socio" and "cnpj_cpf_socio" in df.columns:
        df["cnpj_cpf_socio"] = df.apply(
            lambda r: _mascarar_cpf(str(r["cnpj_cpf_socio"]) if r["cnpj_cpf_socio"] else "")
            if str(r.get("identificador_socio", "2")) == "1"
            else _normalizar_string(r["cnpj_cpf_socio"]),
            axis=1,
        )

    # Parse de datas
    for col_data in COLUNAS_DATA.get(tipo, []):
        if col_data in df.columns:
            df[col_data] = df[col_data].apply(
                lambda v: _parse_data(str(v)) if pd.notna(v) and v else None
            )

    # Acrescenta competencia nas tabelas principais
    if competencia and tipo not in TABELAS_DOMINIO:
        df["competencia"] = competencia

    return df


# ─────────────────────────────────────────────
# CARGA VIA COPY
# ─────────────────────────────────────────────

def _get_raw_connection():
    """Obtém conexão psycopg2 bruta a partir da conexão Django."""
    return connection.connection


def _copy_dataframe(df: pd.DataFrame, tabela: str, colunas: list[str]) -> int:
    """Carrega DataFrame no PostgreSQL usando COPY via psycopg2. Retorna qtd inserida."""
    buf = io.StringIO()
    # Em vez de \N, deixamos string vazia para o NULL
    df[colunas].to_csv(buf, index=False, header=False, sep="\t", na_rep="")
    buf.seek(0)

    conn = _get_raw_connection()
    with conn.cursor() as cur:
        cols_str = ", ".join(colunas)
        # Força o campo vazio ser lido como NULL
        cur.copy_expert(
            f"COPY {tabela} ({cols_str}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '')",
            buf,
        )
    conn.commit()
    return len(df)


# ─────────────────────────────────────────────
# PROCESSAMENTO DE ARQUIVO
# ─────────────────────────────────────────────

def _tipo_do_arquivo(nome_arquivo: str) -> str | None:
    """Identifica o tipo de tabela pelo nome do arquivo."""
    stem = Path(nome_arquivo).stem  # ex: 'Empresas3', 'Cnaes'
    for prefixo, tipo in ARQUIVO_TIPO.items():
        if stem.startswith(prefixo):
            return tipo
    return None


def _processar_arquivo(
    zip_path: Path,
    competencia: str,
    replace: bool,
    log: CargaLog,
    stdout,
    style,
) -> tuple[int, list[str]]:
    """
    Extrai e carrega um arquivo ZIP no banco.
    Retorna (qtd_registros, lista_de_erros).
    """
    tipo = _tipo_do_arquivo(zip_path.name)
    if tipo is None:
        return 0, [f"Tipo não identificado para {zip_path.name}"]

    colunas_base = COLUNAS[tipo]
    tabela_db = DB_TABELA[tipo]
    eh_dominio = tipo in TABELAS_DOMINIO

    # Se --replace e tabela principal, deleta registros da competência (CUIDADO COM LOCKS EM TABELAS GIGANTES)
    if replace:
        if eh_dominio:
            with connection.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {tabela_db} CASCADE")
                stdout.write(f"   ↺  Truncou tabela de domínio: {tabela_db}")
        else:
            # Em tabelas giganes como empresa, DELETE FROM ... WHERE competencia trava o banco se houver FKs.
            # Como workaround temporário, não deletaremos linha a linha aqui neste loop para evitar PostgreSQL lock.
            # O ideal é que o usuário use um comando `limpar_banco` ou `TRUNCATE` global antes do refresh completo.
            stdout.write(f"   ⚠ Aviso: --replace ignorado para {tabela_db} para evitar LOCK. Limpe o banco manualmente se necessário.")

    # Colunas finais (com ou sem competencia)
    colunas_insert = colunas_base.copy()
    if not eh_dominio:
        colunas_insert.append("competencia")

    total = 0
    erros = []

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            # A Receita Federal usa extensões imprevisíveis (ex: FNA.CNAECSV, CSV, etc).
            # Como cada ZIP oficialmente só contém 1 arquivo de dados relevante:
            csv_name = zf.namelist()[0] if zf.namelist() else None
            if not csv_name:
                return 0, [f"Nenhum CSV encontrado em {zip_path.name}"]

            with zf.open(csv_name) as csv_file:
                reader = pd.read_csv(
                    csv_file,
                    sep=";",
                    encoding="iso-8859-1",
                    header=None,
                    names=colunas_base,
                    dtype=str,
                    chunksize=CHUNK_SIZE,
                    on_bad_lines="skip",
                    keep_default_na=False,
                    na_values=[""],
                )

                for i, chunk in enumerate(reader):
                    try:
                        chunk = _transformar_chunk(chunk, tipo, competencia)
                        inseridos = _copy_dataframe(chunk, tabela_db, colunas_insert)
                        total += inseridos
                    except Exception as exc:
                        msg = f"Erro no chunk {i} de {zip_path.name}: {exc}"
                        erros.append(msg)
                        logger.error(msg, exc_info=True)

    except Exception as exc:
        msg = f"Erro ao abrir ZIP {zip_path.name}: {exc}"
        erros.append(msg)
        logger.error(msg, exc_info=True)

    return total, erros


# ─────────────────────────────────────────────
# COMMAND
# ─────────────────────────────────────────────

class Command(BaseCommand):
    help = "Carrega os dados CNPJ de uma competência para o PostgreSQL (ETL via COPY)."

    def add_arguments(self, parser):
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument(
            "--competencia",
            type=str,
            metavar="YYYY-MM",
            help="Competência a carregar (ex: 2025-06)",
        )
        group.add_argument(
            "--all",
            action="store_true",
            default=False,
            help="Carrega todas as competências disponíveis em data/raw/",
        )
        parser.add_argument(
            "--replace",
            action="store_true",
            default=False,
            help="Remove dados existentes antes de inserir (DELETE + INSERT)",
        )

    def handle(self, *args, **options):
        data_dir: Path = getattr(settings, "CNPJ_DATA_DIR", Path("data/raw"))

        if options["all"]:
            # Lista todos os subdiretórios no formato YYYY-MM
            competencias = sorted(
                d.name
                for d in data_dir.iterdir()
                if d.is_dir() and len(d.name) == 7 and d.name[4] == "-"
            )
            if not competencias:
                raise CommandError(f"Nenhuma competência encontrada em {data_dir}")
        else:
            competencias = [options["competencia"]]

        self.stdout.write(
            self.style.SUCCESS(
                f"\n{'='*60}\n"
                f"  Carga CNPJ — {len(competencias)} competência(s)\n"
                f"{'='*60}\n"
            )
        )

        resumo_total = {"arquivos": 0, "registros": 0, "erros": 0}

        for competencia in competencias:
            comp_dir = data_dir / competencia
            if not comp_dir.exists():
                self.stdout.write(
                    self.style.WARNING(f"Diretório não encontrado: {comp_dir}")
                )
                continue

            zips = sorted(comp_dir.glob("*.zip"))
            if not zips:
                self.stdout.write(
                    self.style.WARNING(f"Nenhum ZIP em {comp_dir}")
                )
                continue

            self.stdout.write(
                self.style.HTTP_INFO(f"\n▶  Competência: {competencia} ({len(zips)} arquivos)")
            )

            for zip_path in tqdm(zips, desc=f"  {competencia}", unit="zip", ncols=80):
                log = CargaLog.objects.create(
                    arquivo=zip_path.name,
                    competencia=competencia,
                    status="INICIADO",
                )

                qtd, erros = _processar_arquivo(
                    zip_path,
                    competencia,
                    options["replace"],
                    log,
                    self.stdout,
                    self.style,
                )

                log.qtd_registros = qtd
                log.fim = timezone.now()
                if erros:
                    log.status = "PARCIAL" if qtd > 0 else "ERRO"
                    log.erro = "\n".join(erros[:10])  # Guarda até 10 erros
                else:
                    log.status = "SUCESSO"
                log.save()

                resumo_total["arquivos"] += 1
                resumo_total["registros"] += qtd
                resumo_total["erros"] += len(erros)

                if erros:
                    for e in erros[:3]:
                        self.stdout.write(f"   ⚠  {e}")

        self.stdout.write(
            self.style.SUCCESS(
                f"\n{'='*60}\n"
                f"  ETL Concluído\n"
                f"  Arquivos:   {resumo_total['arquivos']}\n"
                f"  Registros:  {resumo_total['registros']:,}\n"
                f"  Erros:      {resumo_total['erros']}\n"
                f"{'='*60}\n"
            )
        )
