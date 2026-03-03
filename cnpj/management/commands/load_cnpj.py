"""
Management command para carga ETL dos arquivos ZIP de CNPJ no PostgreSQL.

Usa psycopg2 copy_expert para máxima performance de inserção.
Processa múltiplos ZIPs em paralelo com ProcessPoolExecutor.

Uso:
    python manage.py load_cnpj --competencia 2025-06
    python manage.py load_cnpj --all
    python manage.py load_cnpj --competencia 2025-06 --workers 6
    python manage.py load_cnpj --competencia 2025-06 --replace

Modo Lite (economia de espaço/tempo):
    python manage.py load_cnpj --competencia 2026-02 --lite
    python manage.py load_cnpj --competencia 2026-02 --slices 2
    python manage.py load_cnpj --competencia 2026-02 --slices 1 --skip-tables simples socio
"""

import io
import logging
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone
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

TABELAS_DOMINIO = {"cnae", "municipio", "pais", "natureza", "qualificacao", "motivo"}

# Prefixos dos arquivos particionados (para filtro --slices / --skip-tables)
_TIPO_PREFIXO_PARTICIONADO = ["Empresas", "Estabelecimentos", "Socios"]
_TIPO_PARA_PREFIXO = {
    "empresa": "Empresas",
    "estabelecimento": "Estabelecimentos",
    "socio": "Socios",
    "simples": "Simples",
}


def _filtrar_zips(
    zips: list,
    slices: int | None,
    skip_tables: list[str],
) -> list:
    """Filtra a lista de ZIPs para o modo lite.

    Args:
        zips: lista de Path dos ZIPs disponíveis na pasta de competência.
        slices: mantém apenas os N primeiros ZIPs de cada tipo particionado.
        skip_tables: tipos de tabela a ignorar completamente.
    """
    prefixos_skip = {_TIPO_PARA_PREFIXO[t] for t in skip_tables if t in _TIPO_PARA_PREFIXO}

    resultado = []
    for zp in zips:
        nome = zp.name

        # Ignora tipos marcados em --skip-tables
        if any(nome.startswith(p) for p in prefixos_skip):
            continue

        # Aplica filtro de slices apenas para particionados
        if slices is not None and slices < 10:
            for tipo_prefix in _TIPO_PREFIXO_PARTICIONADO:
                if nome.startswith(tipo_prefix):
                    stem = nome[len(tipo_prefix) :].replace(".zip", "")
                    if stem.isdigit() and int(stem) >= slices:
                        break  # índice fora do range → ignorar
            else:
                resultado.append(zp)
                continue
            continue  # saiu pelo break → ignorar

        resultado.append(zp)

    return resultado


COLUNAS_DATA = {
    "empresa": [],
    "estabelecimento": [
        "data_situacao_cadastral",
        "data_inicio_atividade",
        "data_situacao_especial",
    ],
    "socio": ["data_entrada_sociedade"],
    "simples": [
        "data_opcao_simples",
        "data_exclusao_simples",
        "data_opcao_mei",
        "data_exclusao_mei",
    ],
    "cnae": [],
    "municipio": [],
    "pais": [],
    "natureza": [],
    "qualificacao": [],
    "motivo": [],
}

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

CHUNK_SIZE = 150_000  # Aumentado para reduzir overhead de I/O


# ─────────────────────────────────────────────
# FUNÇÕES DE TRANSFORMAÇÃO (vetorizadas)
# ─────────────────────────────────────────────


def _parse_data(serie: pd.Series) -> pd.Series:
    """Converte coluna YYYYMMDD inteira para ISO date (vetorizado)."""
    resultado = pd.to_datetime(
        serie.str.strip().replace({"": None, "0": None, "00000000": None}),
        format="%Y%m%d",
        errors="coerce",
    )
    return resultado.dt.date.astype(object).where(resultado.notna(), other=None)


def _normalizar_col(serie: pd.Series) -> pd.Series:
    """Strip + upper + vazio→None (vetorizado)."""
    s = serie.astype(str).str.strip().str.upper()
    s = s.replace({"NAN": None, "NONE": None, "": None})
    return s


def _transformar_chunk(df: pd.DataFrame, tipo: str, competencia: str | None) -> pd.DataFrame:
    """Aplica todas as transformações no chunk (vetorizado)."""
    colunas_data = COLUNAS_DATA.get(tipo, [])

    for col in df.columns:
        if col not in colunas_data:
            df[col] = _normalizar_col(df[col])

    # Mascarar CPF de sócios PF (vetorizado)
    if tipo == "socio" and "cnpj_cpf_socio" in df.columns:
        mask_pf = df["identificador_socio"] == "1"
        cpf = df.loc[mask_pf, "cnpj_cpf_socio"].astype(str).str.strip()
        df.loc[mask_pf, "cnpj_cpf_socio"] = cpf.where(
            cpf.str.len() != 11,
            "***" + cpf.str[3:9] + "**",
        )

    # Parse de datas (vetorizado por coluna)
    for col_data in colunas_data:
        if col_data in df.columns:
            df[col_data] = _parse_data(df[col_data].astype(str))

    if competencia and tipo not in TABELAS_DOMINIO:
        df["competencia"] = competencia

    return df


# ─────────────────────────────────────────────
# CARGA VIA COPY (conexão independente por worker)
# ─────────────────────────────────────────────


def _get_dsn() -> str:
    """Monta o DSN do banco a partir das configurações do Django."""
    db = settings.DATABASES["default"]
    return (
        f"host={db['HOST']} port={db.get('PORT', 5432)} "
        f"dbname={db['NAME']} user={db['USER']} password={db['PASSWORD']}"
    )


def _copy_dataframe_raw(df: pd.DataFrame, tabela: str, colunas: list[str], dsn: str) -> int:
    """Carrega DataFrame no PostgreSQL via COPY usando conexão psycopg2 própria."""
    buf = io.StringIO()
    df[colunas].to_csv(buf, index=False, header=False, sep="\t", na_rep="")
    buf.seek(0)

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cols_str = ", ".join(colunas)
            cur.copy_expert(
                f"COPY {tabela} ({cols_str}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '')",
                buf,
            )
        conn.commit()
    finally:
        conn.close()

    return len(df)


# ─────────────────────────────────────────────
# LOG EM ARQUIVO
# ─────────────────────────────────────────────


def _log(log_path: str, msg: str) -> None:
    """Grava uma linha no arquivo de log com timestamp e flush imediato."""
    ts = datetime.now().strftime("%H:%M:%S")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")
        f.flush()


# ─────────────────────────────────────────────
# WORKER — executado em processo separado
# ─────────────────────────────────────────────


def _worker(args: tuple) -> tuple[str, int, list[str], float]:
    """
    Worker executado em processo separado pelo ProcessPoolExecutor.
    Recebe (zip_path_str, competencia, replace, dsn, log_path).
    Retorna (zip_name, qtd_registros, lista_erros, elapsed_segundos).
    """
    import time

    zip_path_str, competencia, replace, dsn, log_path = args
    zip_path = Path(zip_path_str)
    t0 = time.monotonic()

    tipo = _tipo_do_arquivo(zip_path.name)
    if tipo is None:
        _log(log_path, f"ERRO\t{zip_path.name}\tTipo não identificado")
        return zip_path.name, 0, [f"Tipo não identificado: {zip_path.name}"], 0.0

    colunas_base = COLUNAS[tipo]
    tabela_db = DB_TABELA[tipo]
    eh_dominio = tipo in TABELAS_DOMINIO

    colunas_insert = colunas_base.copy()
    if not eh_dominio:
        colunas_insert.append("competencia")

    _log(log_path, f"INICIO\t{zip_path.name}\t→ {tabela_db}")

    if replace:
        conn = psycopg2.connect(dsn)
        try:
            with conn.cursor() as cur:
                if eh_dominio:
                    cur.execute(f"TRUNCATE TABLE {tabela_db} CASCADE")
                    _log(log_path, f"TRUNCATE\t{tabela_db}")
            conn.commit()
        finally:
            conn.close()

    total = 0
    erros = []

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            csv_name = zf.namelist()[0] if zf.namelist() else None
            if not csv_name:
                _log(log_path, f"ERRO\t{zip_path.name}\tNenhum CSV no ZIP")
                return zip_path.name, 0, [f"Nenhum CSV em {zip_path.name}"], 0.0

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
                        inseridos = _copy_dataframe_raw(chunk, tabela_db, colunas_insert, dsn)
                        total += inseridos
                        _log(log_path, f"CHUNK\t{zip_path.name}\tchunk={i}  acumulado={total:,}")
                    except Exception as exc:
                        msg = f"Chunk {i} de {zip_path.name}: {exc}"
                        erros.append(msg)
                        _log(log_path, f"ERRO_CHUNK\t{zip_path.name}\t{exc}")

    except Exception as exc:
        erros.append(f"Erro ao abrir {zip_path.name}: {exc}")
        _log(log_path, f"ERRO\t{zip_path.name}\t{exc}")

    elapsed = round(time.monotonic() - t0, 1)
    status = "OK" if not erros else ("PARCIAL" if total > 0 else "ERRO")
    _log(log_path, f"FIM\t{zip_path.name}\t{total:,} reg\t{elapsed}s\t{status}")
    return zip_path.name, total, erros, elapsed


def _tipo_do_arquivo(nome_arquivo: str) -> str | None:
    """Identifica o tipo de tabela pelo nome do arquivo."""
    stem = Path(nome_arquivo).stem
    for prefixo, tipo in ARQUIVO_TIPO.items():
        if stem.startswith(prefixo):
            return tipo
    return None


# ─────────────────────────────────────────────
# COMMAND
# ─────────────────────────────────────────────


class Command(BaseCommand):
    help = "Carrega dados CNPJ no PostgreSQL via COPY, com paralelismo por arquivo ZIP."

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
            help="Remove dados existentes antes de inserir",
        )
        parser.add_argument(
            "--workers",
            type=int,
            default=4,
            metavar="N",
            help="Número de processos paralelos (padrão: 4). Use metade dos CPUs disponíveis.",
        )
        # ── Modo Lite ──────────────────────────────────────────────────────
        parser.add_argument(
            "--slices",
            type=int,
            default=None,
            metavar="N",
            help=(
                "Processa apenas os N primeiros ZIPs de cada tipo particionado "
                "(Empresas, Estabelecimentos, Socios). Range: 1-10. "
                "Exemplo: --slices 1 carrega ~10%% por tipo."
            ),
        )
        parser.add_argument(
            "--skip-tables",
            nargs="+",
            default=[],
            choices=["empresa", "estabelecimento", "socio", "simples"],
            metavar="TIPO",
            help="Ignora os ZIPs dos tipos informados. Ex: --skip-tables simples socio",
        )
        parser.add_argument(
            "--lite",
            action="store_true",
            default=False,
            help="Atalho para --slices 1 --skip-tables simples (mínimo para testes)",
        )

    def handle(self, *args, **options):
        data_dir: Path = getattr(settings, "CNPJ_DATA_DIR", Path("data/raw"))
        workers: int = options["workers"]

        # Configuração Django não sobrevive ao fork; pega DSN antes de criar workers
        dsn = _get_dsn()

        # ── Modo Lite ──────────────────────────────────────────────────────
        slices: int | None = options.get("slices")
        skip_tables: list[str] = list(options.get("skip_tables") or [])

        if options.get("lite"):
            slices = slices or 1
            if "simples" not in skip_tables:
                skip_tables.append("simples")

        if slices is not None and not (1 <= slices <= 10):
            from django.core.management.base import CommandError

            raise CommandError("--slices deve ser um valor entre 1 e 10.")

        modo_info = ""
        if slices is not None or skip_tables:
            partes = []
            if slices is not None:
                partes.append(f"slices={slices}/10")
            if skip_tables:
                partes.append(f"skip={','.join(skip_tables)}")
            modo_info = f"  🔹 MODO LITE ({' | '.join(partes)})\n"

        if options["all"]:
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
                f"{modo_info}"
                f"  Carga CNPJ — {len(competencias)} competência(s) | {workers} workers\n"
                f"{'='*60}\n"
            )
        )

        resumo_total = {"arquivos": 0, "registros": 0, "erros": 0}

        for competencia in competencias:
            comp_dir = data_dir / competencia
            if not comp_dir.exists():
                self.stdout.write(self.style.WARNING(f"Diretório não encontrado: {comp_dir}"))
                continue

            zips = sorted(comp_dir.glob("*.zip"))
            if not zips:
                self.stdout.write(self.style.WARNING(f"Nenhum ZIP em {comp_dir}"))
                continue

            # Aplica filtro lite
            if slices is not None or skip_tables:
                zips_orig = len(zips)
                zips = _filtrar_zips(zips, slices, skip_tables)
                self.stdout.write(
                    self.style.WARNING(f"   ⚡ Lite: {len(zips)}/{zips_orig} arquivos selecionados")
                )

            self.stdout.write(
                self.style.HTTP_INFO(
                    f"\n▶  Competência: {competencia} ({len(zips)} arquivos, {workers} workers)"
                )
            )

            # Cria registros de log no banco (processo principal, antes de fazer fork)
            logs_map: dict[str, CargaLog] = {}
            for zp in zips:
                log = CargaLog.objects.create(
                    arquivo=zp.name,
                    competencia=competencia,
                    status="INICIADO",
                )
                logs_map[zp.name] = log

            # Arquivo de log em tempo real
            log_dir = Path(getattr(settings, "CNPJ_LOGS_DIR", "logs"))
            log_dir.mkdir(parents=True, exist_ok=True)
            ts_inicio = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_path = str(log_dir / f"etl_{competencia}_{ts_inicio}.log")

            _log(log_path, f"=== ETL CNPJ — competência {competencia} | {workers} workers ===")
            _log(log_path, f"Arquivos: {len(zips)}")
            for zp in zips:
                _log(log_path, f"FILA\t{zp.name}")

            # Deriva o caminho relativo ao host (./logs/...) a partir do caminho absoluto do container
            log_nome = Path(log_path).name
            self.stdout.write(
                self.style.WARNING(
                    f"  📄 Log em tempo real:\n"
                    f"     No host  → tail -f ./logs/{log_nome}\n"
                    f"     No container → docker compose exec django tail -f {log_path}"
                )
            )

            # Argumentos para cada worker (inclui log_path)
            tarefas = [(str(zp), competencia, options["replace"], dsn, log_path) for zp in zips]

            concluidos = 0
            total_zips = len(zips)

            with ProcessPoolExecutor(max_workers=workers) as pool:
                futures: dict = {pool.submit(_worker, t): t[0] for t in tarefas}

                tqdm.write(f"\n  {'ARQUIVO':<35} {'REGISTROS':>12}  {'TEMPO':>6}  STATUS")
                tqdm.write(f"  {'-'*68}")

                pbar = tqdm(
                    as_completed(futures),
                    total=total_zips,
                    desc="  total",
                    unit="zip",
                    ncols=72,
                    leave=True,
                )
                for future in pbar:
                    zip_name_str = Path(futures[future]).name
                    log = logs_map[zip_name_str]
                    concluidos += 1

                    try:
                        _, qtd, erros, elapsed = future.result()
                    except Exception as exc:
                        qtd, erros, elapsed = 0, [str(exc)], 0.0

                    log.qtd_registros = qtd
                    log.fim = timezone.now()

                    if erros:
                        log.status = "PARCIAL" if qtd > 0 else "ERRO"
                        log.erro = "\n".join(erros[:10])
                        status_str = "ERRO" if qtd == 0 else "PARCIAL"
                        tqdm.write(f"  {zip_name_str:<35} {qtd:>12,}  {elapsed:>5}s  {status_str}")
                        for e in erros[:2]:
                            tqdm.write(f"      ⚠  {e}")
                    else:
                        log.status = "SUCESSO"
                        tqdm.write(f"  {zip_name_str:<35} {qtd:>12,}  {elapsed:>5}s  OK")
                    log.save()

                    resumo_total["arquivos"] += 1
                    resumo_total["registros"] += qtd
                    resumo_total["erros"] += len(erros)

        resumo_str = (
            f"\n{'='*60}\n"
            f"  ETL Concluído\n"
            f"  Arquivos:   {resumo_total['arquivos']}\n"
            f"  Registros:  {resumo_total['registros']:,}\n"
            f"  Erros:      {resumo_total['erros']}\n"
            f"{'='*60}\n"
        )
        self.stdout.write(self.style.SUCCESS(resumo_str))
        # Grava resumo final no log
        if "log_path" in locals():
            _log(log_path, "=== RESUMO FINAL ===")
            _log(log_path, f"Arquivos:  {resumo_total['arquivos']}")
            _log(log_path, f"Registros: {resumo_total['registros']:,}")
            _log(log_path, f"Erros:     {resumo_total['erros']}")
