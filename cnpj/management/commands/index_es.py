"""
Management command para indexar dados CNPJ do PostgreSQL no Elasticsearch.

Usa JOIN SQL particionado distribuído por N workers (`ProcessPoolExecutor`)
maximizando performance e dividindo a carga em lotes seguros.

Uso:
    python manage.py index_es --competencia 2026-02 --workers 8
    python manage.py index_es --all --workers 16
    python manage.py index_es --competencia 2026-02 --replace --create-index
"""

import logging
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import connection
from tqdm import tqdm

logger = logging.getLogger(__name__)

# Configurações de performance
CHUNK_SIZE_DEFAULT = 2_000  # inserts por bulk ES de cada worker (RAM-friendly)
LOTE_SIZE = 150_000  # docs delegados a 1 worker na pool
MAX_WORKERS_DEFAULT = 4


def _log(log_path: str, msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")
        f.flush()


def _get_competencias_disponiveis() -> list[str]:
    with connection.cursor() as cur:
        cur.execute("SELECT DISTINCT competencia FROM cnpj_estabelecimento ORDER BY competencia")
        return [r[0] for r in cur.fetchall()]


def _count_estabelecimentos(competencia: str) -> int:
    with connection.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM cnpj_estabelecimento WHERE competencia = %s",
            [competencia],
        )
        return cur.fetchone()[0]


# =======================================================================
# LÓGICA DO WORKER (EXECUTADO EM OUTRO PROCESSO NATIVO)
# =======================================================================


def _init_worker():
    """Fecha conexões clonadas do processo pai p/ evitar bugs no psycopg2/Elastic"""
    connection.close()

    # Ao trabalhar com ProcessPool, conexões HTTP persistentes do ES
    # também têm que ser recriadas. Forçamos o reload:
    from elasticsearch_dsl.connections import connections

    try:
        connections.remove_connection("default")
    except Exception:
        pass

    connections.configure(**settings.ELASTICSEARCH_DSL)


def _worker_index_lote(
    lote_id: int,
    competencia: str,
    offset_inicial: int,
    limite: int,
    es_index_name: str,
    batch_size: int,
    log_path: str,
) -> int:
    """
    Função executada pelo ProcessPoolExecutor.
    Lê uma fatia da tabela de `offset_inicial` até `offset_inicial + limite`.
    """
    from elasticsearch.helpers import bulk
    from elasticsearch_dsl.connections import get_connection

    # Cria/Recupera conexão HTTP do Elasticsearch própria da Thread
    es = get_connection()

    sql = """
        SELECT
            e.cnpj_basico,
            e.cnpj_ordem,
            e.cnpj_dv,
            e.nome_fantasia,
            e.situacao_cadastral,
            e.uf,
            e.municipio,
            e.cnae_fiscal_principal,
            emp.porte,
            e.competencia,
            emp.razao_social,
            s.opcao_simples,
            s.opcao_mei
        FROM cnpj_estabelecimento e
        LEFT JOIN cnpj_empresa emp
            ON emp.cnpj_basico = e.cnpj_basico
           AND emp.competencia  = e.competencia
        LEFT JOIN cnpj_simples s
            ON s.cnpj_basico = e.cnpj_basico
           AND s.competencia  = e.competencia
        WHERE e.competencia = %s
        ORDER BY e.id
        LIMIT %s OFFSET %s
    """

    _log(
        log_path,
        f"WORKER-{lote_id}\t{competencia}\tINICIANDO lote {limite:,} a partir do offset {offset_inicial:,}",
    )

    total_indexed = 0
    offset_interno = 0

    with connection.cursor() as cur:
        while offset_interno < limite:
            fetch_size = min(batch_size, limite - offset_interno)

            # Fetch no PG
            cur.execute(sql, [competencia, fetch_size, offset_inicial + offset_interno])
            rows = cur.fetchall()

            if not rows:
                break

            # Transforma rows em Doc Dicts do ES
            actions = []
            for row in rows:
                (
                    cnpj_b,
                    cnpj_o,
                    cnpj_dv,
                    nome_fantasia,
                    situacao,
                    uf,
                    municipio,
                    cnae,
                    porte,
                    comp,
                    razao_social,
                    opcao_simples,
                    opcao_mei,
                ) = row

                # ID único combinando CNPJ 14 + mês (evita conflitos no ES)
                doc_id = f"{cnpj_b or ''}{cnpj_o or ''}{cnpj_dv or ''}_{comp or ''}"

                actions.append(
                    {
                        "_index": es_index_name,
                        "_id": doc_id,
                        "_source": {
                            "cnpj_basico": cnpj_b or "",
                            "cnpj_ordem": cnpj_o or "",
                            "cnpj_dv": cnpj_dv or "",
                            "razao_social": razao_social or "",
                            "nome_fantasia": nome_fantasia or "",
                            "situacao_cadastral": situacao or "",
                            "uf": uf or "",
                            "municipio": municipio or "",
                            "cnae_fiscal_principal": cnae or "",
                            "porte": porte or "",
                            "competencia": comp or "",
                            "opcao_simples": opcao_simples or "",
                            "opcao_mei": opcao_mei or "",
                        },
                    }
                )

            # Dispara Bulk no Elasticsearch
            try:
                success, errors = bulk(
                    es,
                    actions,
                    raise_on_error=False,
                    request_timeout=60,
                    chunk_size=fetch_size,
                )
                total_indexed += success
                if errors:
                    _log(log_path, f"WORKER-{lote_id}\tBULK_ERRORS\t{len(errors)} erros")
            except Exception as exc:
                _log(log_path, f"WORKER-{lote_id}\tBULK_EXCEPTION\t{exc}")

            offset_interno += len(rows)

    _log(log_path, f"WORKER-{lote_id}\tFIM\tIndexou {total_indexed:,} docs.")
    return total_indexed


# =======================================================================
# LÓGICA DO MASTER (DELEGA E VÊ PROGRESSO)
# =======================================================================


def _index_competencia_paralelo(
    competencia: str,
    replace: bool,
    es_index_name: str,
    chunk_size: int,
    workers: int,
    log_path: str,
) -> int:
    from elasticsearch_dsl.connections import get_connection

    es = get_connection()

    if replace:
        _log(log_path, f"DELETE_BY_QUERY\t{competencia}\tapagando docs anteriores...")
        try:
            es.delete_by_query(
                index=es_index_name,
                body={"query": {"term": {"competencia": competencia}}},
                conflicts="proceed",
                refresh=True,
            )
            _log(log_path, f"DELETE_BY_QUERY\t{competencia}\tOK")
        except Exception as e:
            _log(log_path, f"DELETE_BY_QUERY\t{competencia}\tErro ou índice vazio ({e})")

    # Quantos registros existem no PG pra essa competência
    total_rows = _count_estabelecimentos(competencia)
    _log(log_path, f"MASTER\t{competencia}\tTotal detectado no DB: {total_rows:,} estabelecimentos")

    if total_rows == 0:
        return 0

    # Quebra de Lotes
    num_lotes = (total_rows + LOTE_SIZE - 1) // LOTE_SIZE
    lotes_args = []

    for i in range(num_lotes):
        offset = i * LOTE_SIZE
        limite = LOTE_SIZE if (offset + LOTE_SIZE) <= total_rows else (total_rows - offset)
        lotes_args.append((i, competencia, offset, limite, es_index_name, chunk_size, log_path))

    total_indexed = 0

    # Cria Pool Paralelo (N Workers)
    with ProcessPoolExecutor(max_workers=workers, initializer=_init_worker) as executor:
        # Agenda tudo
        futures = {}
        for args in lotes_args:
            future = executor.submit(_worker_index_lote, *args)
            futures[future] = args

        # Barra de Progresso Master
        pbar = tqdm(
            total=total_rows,
            desc=f"  {competencia} [{workers}W]",
            unit="doc",
            ncols=80,
            leave=True,
        )

        for future in as_completed(futures):
            args = futures[future]
            lote_id, _, _, limite, _, _, _ = args
            try:
                qtd_lote = future.result()
                total_indexed += qtd_lote
                pbar.update(limite)
            except Exception as exc:
                _log(log_path, f"MASTER\tWORKER-{lote_id} ESTOUROU: {exc}")
                pbar.update(limite)  # Atualiza do mesmo jeito p/ barra não travar

        pbar.close()

    return total_indexed


class Command(BaseCommand):
    help = "Indexa dados CNPJ do PostgreSQL no Elasticsearch usando multiprocessamento rápido."

    def add_arguments(self, parser):
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument(
            "--competencia",
            type=str,
            metavar="YYYY-MM",
            help="Competência a indexar (ex: 2026-02)",
        )
        group.add_argument(
            "--all",
            action="store_true",
            default=False,
            help="Indexa todas as competências disponíveis no banco",
        )
        parser.add_argument(
            "--replace",
            action="store_true",
            default=False,
            help="Apaga documentos existentes da competência antes de reindexar",
        )
        parser.add_argument(
            "--create-index",
            action="store_true",
            default=False,
            help="Cria (ou recria) o índice ES antes de indexar",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=CHUNK_SIZE_DEFAULT,
            metavar="N",
            help=f"Registros por requisição bulk no ES (padrão: {CHUNK_SIZE_DEFAULT}).",
        )
        parser.add_argument(
            "--workers",
            type=int,
            default=MAX_WORKERS_DEFAULT,
            metavar="N",
            help=f"Qtd. de processos independentes (padrão: {MAX_WORKERS_DEFAULT}).",
        )

    def handle(self, *args, **options):
        try:
            from cnpj.documents import EstabelecimentoDocument
        except Exception as exc:
            raise CommandError(f"Erro ao importar EstabelecimentoDocument: {exc}") from exc

        es_index_name: str = getattr(settings, "CNPJ_ES_INDEX", "cnpj_estabelecimentos")
        chunk_size: int = options["batch_size"]
        workers: int = options["workers"]

        if options["all"]:
            competencias = _get_competencias_disponiveis()
            if not competencias:
                raise CommandError("Nenhuma competência encontrada no banco.")
        else:
            competencias = [options["competencia"]]

        # ── Cria/recria índice ES ────────────────────────────────────────────────
        if options["create_index"]:
            self.stdout.write(self.style.WARNING("  🔧 Recriando índice ES..."))
            try:
                EstabelecimentoDocument._index.delete(ignore=404)
                EstabelecimentoDocument.init()
                self.stdout.write(
                    self.style.SUCCESS(f"  ✔ Índice '{es_index_name}' recriado do zero.")
                )
            except Exception as exc:
                raise CommandError(f"Erro ao criar índice: {exc}") from exc
        else:
            try:
                EstabelecimentoDocument.init()  # Certifica
            except Exception as exc:
                self.stdout.write(self.style.WARNING(f"  ⚠  Não pre-iniciou o index: {exc}"))

        # ── Setup de Log ─────────────────────────────────────────────────────────
        log_dir = Path(getattr(settings, "CNPJ_LOGS_DIR", "logs"))
        log_dir.mkdir(parents=True, exist_ok=True)
        ts_inicio = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = str(log_dir / f"index_parallel_{ts_inicio}.log")
        log_nome = Path(log_path).name

        self.stdout.write(
            self.style.SUCCESS(
                f"\n{'='*60}\n"
                f"  🚀 INDEX ES PARALELO \n"
                f"  Competências: {len(competencias)}\n"
                f"  Índice:       {es_index_name}\n"
                f"  Workers:      {workers} CPUs\n"
                f"  Batch/Worker: {chunk_size:,} docs/request\n"
                f"{'='*60}\n"
            )
        )
        self.stdout.write(
            self.style.WARNING(
                f"  📄 Log em tempo real:\n"
                f"     No host      → tail -f ./logs/{log_nome}\n"
                f"     No container → docker compose exec django tail -f {log_path}"
            )
        )

        _log(
            log_path,
            f"=== INICIO INDEX ES P-POOL | index={es_index_name} | workers={workers} | batch={chunk_size} ===",
        )

        total_geral = 0
        t0 = time.monotonic()

        for competencia in competencias:
            self.stdout.write(self.style.HTTP_INFO(f"\n▶  Competência Atual: {competencia}"))
            t_comp = time.monotonic()

            try:
                qtd = _index_competencia_paralelo(
                    competencia=competencia,
                    replace=options["replace"],
                    es_index_name=es_index_name,
                    chunk_size=chunk_size,
                    workers=workers,
                    log_path=log_path,
                )
            except Exception as exc:
                self.stdout.write(self.style.ERROR(f"  ERRO GERAL em {competencia}: {exc}"))
                _log(log_path, f"ERRO\t{competencia}\t{exc}")
                continue

            elapsed = round(time.monotonic() - t_comp, 1)
            total_geral += qtd
            _log(log_path, f"MASTER\tFIM\t{competencia}\t{qtd:,} docs em {elapsed}s")
            self.stdout.write(
                self.style.SUCCESS(f"  ✔ Concluído {competencia}: {qtd:,} indexados em {elapsed}s")
            )

        elapsed_total = round(time.monotonic() - t0, 1)
        self.stdout.write(
            self.style.SUCCESS(
                f"\n{'='*60}\n"
                f"  🎉 FIM DA INDEXAÇÃO\n"
                f"  Total Indexado: {total_geral:,} documentos\n"
                f"  Tempo Total:    {elapsed_total}s ({(elapsed_total/60):.2f} min)\n"
                f"{'='*60}\n"
            )
        )
        _log(
            log_path, f"=== RESUMO FINAL: {total_geral:,} docs consolidados em {elapsed_total}s ==="
        )
