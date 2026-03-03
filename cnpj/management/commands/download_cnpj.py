"""
Management command para download automatizado dos arquivos ZIP da
base de dados CNPJ da Receita Federal do Brasil.

Uso:
    python manage.py download_cnpj --start 2025-01 --end 2026-01
    python manage.py download_cnpj --only-latest
    python manage.py download_cnpj --start 2025-06 --end 2025-06

Modo Lite (economia de espaço/tempo):
    python manage.py download_cnpj --only-latest --slices 1
    python manage.py download_cnpj --only-latest --slices 2 --skip-tables simples
    python manage.py download_cnpj --only-latest --lite
"""

import logging
import time
from datetime import date
from pathlib import Path

import requests
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from tqdm import tqdm

BASE_URL = "https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9"

# Todos os 33 arquivos disponíveis por competência
FILES_DOMINIO = [
    "Cnaes.zip",
    "Motivos.zip",
    "Municipios.zip",
    "Naturezas.zip",
    "Paises.zip",
    "Qualificacoes.zip",
    "Simples.zip",
]
FILES_PARTICIONADOS = (
    [f"Empresas{i}.zip" for i in range(10)]
    + [f"Estabelecimentos{i}.zip" for i in range(10)]
    + [f"Socios{i}.zip" for i in range(10)]
)
ALL_FILES = FILES_DOMINIO + FILES_PARTICIONADOS

# Mapeamento tipo → prefixo de arquivo (para --skip-tables)
TIPO_PREFIXO = {
    "empresa": "Empresas",
    "estabelecimento": "Estabelecimentos",
    "socio": "Socios",
    "simples": "Simples",
}


def _filtrar_arquivos(
    arquivos: list[str],
    slices: int | None,
    skip_tables: list[str],
) -> list[str]:
    """Filtra a lista de arquivos para o modo lite.

    Args:
        arquivos: lista completa de arquivos a baixar.
        slices: se informado, mantém apenas os N primeiros ZIPs de cada
                tipo particionado (0..N-1). Domínio não é afetado.
        skip_tables: lista de tipos a ignorar (ex: ["simples", "socio"]).
    """
    prefixos_skip = {TIPO_PREFIXO[t] for t in skip_tables if t in TIPO_PREFIXO}

    resultado = []
    for arq in arquivos:
        # Verifica se o arquivo pertence a um tipo ignorado
        if any(arq.startswith(p) for p in prefixos_skip):
            continue

        # Aplica filtro de slices apenas para particionados
        if slices is not None and slices < 10:
            for tipo_prefix in ["Empresas", "Estabelecimentos", "Socios"]:
                if arq.startswith(tipo_prefix):
                    # Extrai o índice numérico do arquivo (ex: Empresas3.zip → 3)
                    stem = arq[len(tipo_prefix) :].replace(".zip", "")
                    if stem.isdigit() and int(stem) >= slices:
                        break
            else:
                resultado.append(arq)
                continue
            continue  # índice >= slices → ignorar

        resultado.append(arq)

    return resultado


def _competencias_no_intervalo(start: str, end: str) -> list[str]:
    """Retorna lista de competências YYYY-MM entre start e end (inclusive)."""

    def _to_ym(s):
        y, m = s.split("-")
        return int(y), int(m)

    sy, sm = _to_ym(start)
    ey, em = _to_ym(end)

    resultado = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        resultado.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return resultado


def _competencia_mais_recente() -> str:
    """Retorna a competência mais recente (mês atual - 1)."""
    hoje = date.today()
    m = hoje.month - 1
    y = hoje.year
    if m == 0:
        m = 12
        y -= 1
    return f"{y:04d}-{m:02d}"


def _download_arquivo(url: str, dest: Path, logger: logging.Logger, max_retries: int = 3) -> bool:
    """Baixa um arquivo com retry e backoff exponencial. Retorna True em sucesso."""
    # Skip se já existe e tem tamanho > 0
    if dest.exists() and dest.stat().st_size > 0:
        logger.info(f"SKIP (já existe): {dest.name}")
        return True

    for tentativa in range(1, max_retries + 1):
        try:
            resp = requests.get(url, stream=True, timeout=120)
            resp.raise_for_status()

            total = int(resp.headers.get("content-length", 0))
            dest.parent.mkdir(parents=True, exist_ok=True)

            with (
                open(dest, "wb") as f,
                tqdm(
                    total=total,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=f"  {dest.name}",
                    leave=False,
                    ncols=80,
                ) as pbar,
            ):
                for chunk in resp.iter_content(chunk_size=1024 * 256):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))

            logger.info(f"OK: {dest.name}")
            return True

        except Exception as exc:
            espera = 2**tentativa
            logger.warning(
                f"Tentativa {tentativa}/{max_retries} falhou para {dest.name}: {exc}. "
                f"Aguardando {espera}s..."
            )
            # Remove arquivo parcial
            if dest.exists():
                dest.unlink()
            if tentativa < max_retries:
                time.sleep(espera)

    logger.error(f"FALHA DEFINITIVA: {dest.name}")
    return False


class Command(BaseCommand):
    help = "Baixa os arquivos ZIP de CNPJ da Receita Federal para o intervalo de competências informado."

    def add_arguments(self, parser):
        parser.add_argument(
            "--start",
            type=str,
            default="2025-01",
            metavar="YYYY-MM",
            help="Competência inicial (padrão: 2025-01)",
        )
        parser.add_argument(
            "--end",
            type=str,
            default="2026-01",
            metavar="YYYY-MM",
            help="Competência final (padrão: 2026-01)",
        )
        parser.add_argument(
            "--only-latest",
            action="store_true",
            default=False,
            help="Baixa apenas a competência mais recente (ignora --start/--end)",
        )
        parser.add_argument(
            "--files",
            nargs="+",
            default=None,
            metavar="ARQUIVO",
            help="Baixa apenas os arquivos especificados (ex: Cnaes.zip Simples.zip)",
        )
        # ── Modo Lite ──────────────────────────────────────────────────────
        parser.add_argument(
            "--slices",
            type=int,
            default=None,
            metavar="N",
            help=(
                "Baixa apenas os N primeiros ZIPs de cada tipo particionado "
                "(Empresas, Estabelecimentos, Socios). Range: 1-10. "
                "Exemplo: --slices 1 baixa ~10%% dos cadastros."
            ),
        )
        parser.add_argument(
            "--skip-tables",
            nargs="+",
            default=[],
            choices=["empresa", "estabelecimento", "socio", "simples"],
            metavar="TIPO",
            help="Ignora os arquivos dos tipos informados. Ex: --skip-tables simples socio",
        )
        parser.add_argument(
            "--lite",
            action="store_true",
            default=False,
            help="Atalho para --slices 1 --skip-tables simples (mínimo para testes)",
        )

    def handle(self, *args, **options):
        data_dir: Path = getattr(settings, "CNPJ_DATA_DIR", Path("data/raw"))
        logs_dir: Path = getattr(settings, "CNPJ_LOGS_DIR", Path("logs"))
        logs_dir.mkdir(parents=True, exist_ok=True)

        if options["only_latest"]:
            competencias = [_competencia_mais_recente()]
        else:
            try:
                competencias = _competencias_no_intervalo(options["start"], options["end"])
            except ValueError as exc:
                raise CommandError(f"Intervalo inválido: {exc}") from exc

        # ── Modo Lite ──────────────────────────────────────────────────────
        slices = options.get("slices")
        skip_tables: list[str] = options.get("skip_tables") or []

        if options.get("lite"):
            slices = slices or 1
            if "simples" not in skip_tables:
                skip_tables = list(skip_tables) + ["simples"]

        if slices is not None and not (1 <= slices <= 10):
            raise CommandError("--slices deve ser um valor entre 1 e 10.")

        if options["files"]:
            arquivos = options["files"]
        else:
            arquivos = _filtrar_arquivos(ALL_FILES, slices, skip_tables)

        modo_info = ""
        if slices is not None or skip_tables:
            partes = []
            if slices is not None:
                partes.append(f"slices={slices}/10")
            if skip_tables:
                partes.append(f"skip={','.join(skip_tables)}")
            modo_info = f"  🔹 MODO LITE ({' | '.join(partes)})\n"

        self.stdout.write(
            self.style.SUCCESS(
                f"\n{'='*60}\n"
                f"{modo_info}"
                f"  Download CNPJ RF — {len(competencias)} competência(s)\n"
                f"  Arquivos por competência: {len(arquivos)}\n"
                f"  Total de downloads: {len(competencias) * len(arquivos)}\n"
                f"{'='*60}\n"
            )
        )

        total_ok = 0
        total_erros = 0

        for competencia in tqdm(competencias, desc="Competências", unit="comp", ncols=80):
            # Logger por competência
            log_path = logs_dir / f"download_{competencia}.log"
            logger = logging.getLogger(f"download.{competencia}")
            if not logger.handlers:
                handler = logging.FileHandler(log_path, encoding="utf-8")
                handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
                logger.addHandler(handler)
                logger.setLevel(logging.DEBUG)

            self.stdout.write(f"\n▶  Competência: {competencia}")
            comp_ok = 0
            comp_erros = 0

            for arquivo in arquivos:
                url = f"{BASE_URL}/{competencia}/{arquivo}"
                dest = data_dir / competencia / arquivo
                ok = _download_arquivo(url, dest, logger)
                if ok:
                    comp_ok += 1
                    total_ok += 1
                else:
                    comp_erros += 1
                    total_erros += 1

            status_str = self.style.SUCCESS(f"✓ {comp_ok}") + (
                f" | {self.style.ERROR(f'✗ {comp_erros}')}" if comp_erros else ""
            )
            self.stdout.write(f"   Resultado: {status_str}")

        self.stdout.write(
            self.style.SUCCESS(
                f"\n{'='*60}\n"
                f"  Concluído — Sucesso: {total_ok} | Erros: {total_erros}\n"
                f"{'='*60}\n"
            )
        )
