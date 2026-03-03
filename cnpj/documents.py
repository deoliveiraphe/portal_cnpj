"""
Documento Elasticsearch para indexação de Estabelecimentos CNPJ.

O índice `cnpj_estabelecimentos` armazena os campos mais usados em busca,
incluindo razão social (proveniente da tabela Empresa via join).

Mapeamento:
  - Campos de texto livre (razao_social, nome_fantasia): text + keyword (multi-field)
  - Demais campos: keyword (busca exata/filtragem)
"""

from django.conf import settings
from django_elasticsearch_dsl import Document, fields
from django_elasticsearch_dsl.registries import registry

# Import lazy para evitar circular import no momento do carregamento do app
from cnpj.models import Estabelecimento


@registry.register_document
class EstabelecimentoDocument(Document):
    """
    Documento ES para Estabelecimento.

    O campo `razao_social` não existe em Estabelecimento — é preenchido
    via `get_queryset` + `prepare_razao_social` a partir da tabela Empresa.
    """

    # ── campos de busca textual ─────────────────────────────────────────────
    razao_social = fields.TextField(
        attr="razao_social_es",
        fields={"keyword": fields.KeywordField()},
    )
    nome_fantasia = fields.TextField(
        fields={"keyword": fields.KeywordField()},
    )

    # ── campos de filtro (keyword) ──────────────────────────────────────────
    cnpj_basico = fields.KeywordField()
    cnpj_ordem = fields.KeywordField()
    cnpj_dv = fields.KeywordField()
    situacao_cadastral = fields.KeywordField()
    uf = fields.KeywordField()
    municipio = fields.KeywordField()
    cnae_fiscal_principal = fields.KeywordField()
    porte = fields.KeywordField()
    competencia = fields.KeywordField()

    # ── campos Simples/MEI (vindos do modelo via prepare_*) ─────────────────
    opcao_simples = fields.KeywordField()
    opcao_mei = fields.KeywordField()

    class Index:
        name = getattr(settings, "CNPJ_ES_INDEX", "cnpj_estabelecimentos")
        settings = {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "30s",
        }

    class Django:
        model = Estabelecimento
        # Não usamos auto_sync (sinal de save) pois a carga é em bulk via COPY
        ignore_signals = True
        auto_refresh = False

    def get_queryset(self):
        """
        Retorna queryset base com select_related para evitar N+1 nas
        chamadas de prepare_*.  O índice é alimentado explicitamente
        pelo comando `index_es`, não por sinais automáticos.
        """
        return (
            super()
            .get_queryset()
            .select_related("cnpj_basico_empresa")
            .only(
                "id",
                "cnpj_basico",
                "cnpj_ordem",
                "cnpj_dv",
                "nome_fantasia",
                "situacao_cadastral",
                "uf",
                "municipio",
                "cnae_fiscal_principal",
                "porte",
                "competencia",
            )
        )

    # ── prepare_* são chamados pelo indexador do comando index_es ────────────

    def prepare_razao_social_es(self, instance):
        """Razão social vinda do atributo injetado pelo comando index_es."""
        return getattr(instance, "_razao_social", "") or ""

    def prepare_opcao_simples(self, instance):
        return getattr(instance, "_opcao_simples", "") or ""

    def prepare_opcao_mei(self, instance):
        return getattr(instance, "_opcao_mei", "") or ""
