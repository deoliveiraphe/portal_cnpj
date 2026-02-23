"""
API REST do Portal CNPJ — Receita Federal do Brasil

Endpoints:
  GET /api/stats/               — estatísticas gerais (home)
  GET /api/competencias/        — lista competências disponíveis
  GET /api/busca/               — busca com filtros + paginação
  GET /api/cnpj/<cnpj_basico>/  — detalhe completo de empresa
"""
import json
import time
from django.http import JsonResponse
from django.views.decorators.http import require_GET
from django.views.decorators.cache import cache_page
from django.db.models import Count, Q, Exists, OuterRef
from django.core.paginator import Paginator

from .models import (
    Empresa, Estabelecimento, Socio, Simples,
    Cnae, Municipio, Natureza, Qualificacao, CargaLog,
)

PAGE_SIZE = 25

# ── helpers ────────────────────────────────────────────────────────────────

def _fmt_date(d):
    """Date → DD/MM/AAAA ou ''."""
    if not d:
        return ""
    try:
        return d.strftime("%d/%m/%Y")
    except Exception:
        return str(d)


def _latest_competencia():
    """Retorna a competência mais recente com dados no banco."""
    comp = (
        Estabelecimento.objects.values_list("competencia", flat=True)
        .distinct()
        .order_by("-competencia")
        .first()
    )
    return comp or ""


SITUACAO_LABEL = {
    "01": "NULA",
    "02": "ATIVA",
    "03": "SUSPENSA",
    "04": "INAPTA",
    "08": "BAIXADA",
}

PORTE_LABEL = {
    "00": "Não informado",
    "01": "Micro Empresa",
    "03": "Pequeno Porte",
    "05": "Demais",
}

IDENTIFICADOR_SOCIO_LABEL = {
    "1": "PF",
    "2": "PJ",
    "3": "Estrangeiro",
}

FAIXA_ETARIA_LABEL = {
    "0": "Não informado",
    "1": "0-12 anos",
    "2": "13-20 anos",
    "3": "21-30 anos",
    "4": "31-40 anos",
    "5": "41-50 anos",
    "6": "51-60 anos",
    "7": "61-70 anos",
    "8": "71-80 anos",
    "9": "81+ anos",
}


def _format_cnpj(basico, ordem="0001", dv="00"):
    """Formata CNPJ no padrão XX.XXX.XXX/XXXX-XX."""
    b = (basico or "").zfill(8)
    o = (ordem or "0001").zfill(4)
    d = (dv or "00").zfill(2)
    return f"{b[:2]}.{b[2:5]}.{b[5:8]}/{o}-{d}"


# ── endpoints ──────────────────────────────────────────────────────────────

@require_GET
@cache_page(60 * 60 * 24)
def api_stats(request):
    """GET /api/stats/ — estatísticas gerais."""
    competencias = sorted(
        Estabelecimento.objects.values_list("competencia", flat=True).distinct(),
        reverse=True,
    )
    ultima = competencias[0] if competencias else None
    total_empresas = (
        Estabelecimento.objects.filter(competencia=ultima).count() if ultima else 0
    )
    cargas_ok = CargaLog.objects.filter(status="SUCESSO").count()

    return JsonResponse({
        "total_empresas": total_empresas,
        "total_competencias": len(competencias),
        "ultima_competencia": ultima,
        "cargas_concluidas": cargas_ok,
    })


@require_GET
@cache_page(60 * 60 * 24)
def api_competencias(request):
    """GET /api/competencias/ — lista competências disponíveis."""
    comps = sorted(
        Estabelecimento.objects.values_list("competencia", flat=True).distinct(),
        reverse=True,
    )
    return JsonResponse({"competencias": list(comps)})


@require_GET
def api_busca(request):
    """
    GET /api/busca/ — busca com filtros + paginação.

    Query params:
      q            — razão social ou CNPJ (busca livre)
      competencia  — YYYY-MM (padrão: mais recente)
      uf           — sigla UF
      municipio    — código do município
      cnae         — código CNAE principal
      situacao     — código situação (02=Ativa, 04=Inapta…)
      porte        — código porte (01, 03, 05)
      simples      — S ou N
      mei          — S ou N
      page         — página (padrão: 1)
    """
    t0 = time.time()

    competencia = request.GET.get("competencia") or _latest_competencia()
    if not competencia:
        return JsonResponse({"results": [], "total": 0, "paginas": 0}, status=200)

    # Inicializa Busca do Elasticsearch
    s = EstabelecimentoDocument.search()
    
    # Filtros exatos — usando Match (campos mapeados como text no índice atual)
    must_queries = [Match(competencia=competencia)]

    q = request.GET.get("q", "").strip()
    is_cnpj_search = False  # default — sobrescrito abaixo se houver termo de busca
    if q:
        cnpj_limpo = ''.join(filter(str.isdigit, q))
        is_cnpj_search = (cnpj_limpo and len(cnpj_limpo) >= 3 and cnpj_limpo == q.replace(".", "").replace("/", "").replace("-", "").strip())
        
        if is_cnpj_search:
            must_queries.append(Prefix(cnpj_basico=cnpj_limpo[:8]))
        else:
            # MultiMatch Textual nos campos principais
            must_queries.append(MultiMatch(
                query=q, 
                fields=['razao_social', 'nome_fantasia'],
                type='best_fields',
                operator='and'
            ))

    if uf := request.GET.get("uf", "").strip().upper():
        must_queries.append(Match(uf=uf))

    if municipio := request.GET.get("municipio", "").strip():
        must_queries.append(Match(municipio=municipio))

    if cnae := request.GET.get("cnae", "").strip():
        must_queries.append(Prefix(cnae_fiscal_principal=cnae[:7]))

    if situacao := request.GET.get("situacao", "").strip():
        must_queries.append(Match(situacao_cadastral=situacao))

    if porte := request.GET.get("porte", "").strip():
        must_queries.append(Match(porte=porte))

    simples = request.GET.get("simples", "").strip().upper()
    if simples in ("S", "N"):
        # campo booleano indexado como bool — string 'true'/'false'
        must_queries.append(Match(opcao_simples='true' if simples == 'S' else 'false'))

    mei = request.GET.get("mei", "").strip().upper()
    if mei in ("S", "N"):
        must_queries.append(Match(opcao_mei='true' if mei == 'S' else 'false'))

    # Aplica todas as condições `AND` conjuntas
    s = s.query(Bool(must=must_queries))
    
    # Sem sort explícito — ES retorna por score de relevância (BM25), adequado para todos os casos

    # Paginação NoSQL nativa do Elasticsearch (From / Size)
    page = int(request.GET.get("page", 1))
    start = (page - 1) * PAGE_SIZE
    s = s[start:start + PAGE_SIZE]

    # Dispara Query ao Cluster Elastic
    print(f"Tempo pre query Elastic(): {time.time() - t0:.4f}s", flush=True)
    response = s.execute()
    total = response.hits.total.value
    print(f"Tempo pos query Elastic(): {time.time() - t0:.4f}s", flush=True)
    
    # Paginator helper compatível com formato nativo da view
    num_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE

    # Mapas de descrição a partir do resultado do Elasticsearch (Hit objects)
    es_results = list(response)
    
    cnae_codigos = {e.cnae_fiscal_principal for e in es_results if hasattr(e, 'cnae_fiscal_principal') and e.cnae_fiscal_principal}
    cnae_map = dict(Cnae.objects.filter(codigo__in=cnae_codigos).values_list("codigo", "descricao"))
    
    mun_codigos = {e.municipio for e in es_results if hasattr(e, 'municipio') and e.municipio}
    mun_map = dict(Municipio.objects.filter(codigo__in=mun_codigos).values_list("codigo", "descricao"))

    results = []
    for e in es_results:
        # A view achatada do ES possui os dados do doc diretamente no nó raiz
        cnae_desc = cnae_map.get(e.cnae_fiscal_principal, "") if hasattr(e, 'cnae_fiscal_principal') else ""
        mun_desc = mun_map.get(e.municipio, "") if hasattr(e, 'municipio') else ""
        
        results.append({
            "cnpj_basico": e.cnpj_basico if hasattr(e, 'cnpj_basico') else "",
            "cnpj": _format_cnpj(
                e.cnpj_basico if hasattr(e, 'cnpj_basico') else "", 
                e.cnpj_ordem if hasattr(e, 'cnpj_ordem') else "", 
                e.cnpj_dv if hasattr(e, 'cnpj_dv') else ""
            ),
            "razao_social": getattr(e, 'razao_social', ""),
            "nome_fantasia": getattr(e, 'nome_fantasia', ""),
            "situacao": SITUACAO_LABEL.get(getattr(e, 'situacao_cadastral', ""), getattr(e, 'situacao_cadastral', "")),
            "situacao_codigo": getattr(e, 'situacao_cadastral', ""),
            "municipio": mun_desc,
            "municipio_codigo": getattr(e, 'municipio', ""),
            "uf": getattr(e, 'uf', ""),
            "cnae_principal": getattr(e, 'cnae_fiscal_principal', ""),
            "cnae_descricao": cnae_desc,
            "porte": PORTE_LABEL.get(getattr(e, 'porte', ""), ""),
        })

    elapsed = round(time.time() - t0, 3)
    print(f"Tempo TOTAL view: {elapsed}s", flush=True)
    return JsonResponse({
        "results": results,
        "total": total,
        "pagina": page,
        "paginas": num_pages,
        "competencia": competencia,
        "elapsed": elapsed,
    })


@require_GET
def api_cnpj_detalhe(request, cnpj_basico):
    """
    GET /api/cnpj/<cnpj_basico>/ — detalhe completo de empresa.

    Query params:
      competencia — YYYY-MM (padrão: mais recente)
    """
    cnpj_basico = cnpj_basico.replace(".", "").replace("/", "").replace("-", "").zfill(8)
    competencia = request.GET.get("competencia") or _latest_competencia()

    if not competencia:
        return JsonResponse({"error": "Nenhuma competência disponível."}, status=404)

    # Estabelecimento matriz (ordem=0001)
    try:
        estab = Estabelecimento.objects.get(
            cnpj_basico=cnpj_basico,
            cnpj_ordem="0001",
            competencia=competencia,
        )
    except Estabelecimento.DoesNotExist:
        # Fallback: qualquer estab deste CNPJ
        estab = Estabelecimento.objects.filter(
            cnpj_basico=cnpj_basico, competencia=competencia
        ).order_by("cnpj_ordem").first()
        if not estab:
            return JsonResponse({"error": "CNPJ não encontrado."}, status=404)

    # Empresa
    empresa = Empresa.objects.filter(
        cnpj_basico=cnpj_basico, competencia=competencia
    ).first()

    # Sócios
    socios_qs = Socio.objects.filter(cnpj_basico=cnpj_basico, competencia=competencia)
    qual_map = dict(Qualificacao.objects.values_list("codigo", "descricao"))
    nat_map = dict(Natureza.objects.values_list("codigo", "descricao"))

    socios = []
    for s in socios_qs:
        socios.append({
            "nome": s.nome_socio or "",
            "tipo": IDENTIFICADOR_SOCIO_LABEL.get(s.identificador_socio or "", "PF"),
            "cpfCnpj": s.cnpj_cpf_socio or "",
            "qualificacao": qual_map.get(s.qualificacao_socio or "", s.qualificacao_socio or ""),
            "dataEntrada": _fmt_date(s.data_entrada_sociedade),
            "faixaEtaria": FAIXA_ETARIA_LABEL.get(s.faixa_etaria or "", ""),
            "representanteLegal": {
                "nome": s.nome_representante or "",
                "cpf": s.representante_legal or "",
                "qualificacao": qual_map.get(s.qualificacao_representante or "", ""),
            } if s.representante_legal else None,
        })

    # Simples / MEI
    simples_obj = Simples.objects.filter(
        cnpj_basico=cnpj_basico, competencia=competencia
    ).first()

    # CNAEs
    cnae_map = dict(Cnae.objects.values_list("codigo", "descricao"))
    cnae_principal = estab.cnae_fiscal_principal or ""
    cnae_principal_desc = cnae_map.get(cnae_principal, "")

    cnae_secundarios = []
    if estab.cnae_fiscal_secundaria:
        for cod in estab.cnae_fiscal_secundaria.replace(",", " ").split():
            cod = cod.strip().zfill(7)
            if cod:
                cnae_secundarios.append({
                    "codigo": cod,
                    "descricao": cnae_map.get(cod, ""),
                })

    # Município
    mun_map = dict(Municipio.objects.values_list("codigo", "descricao"))
    municipio_desc = mun_map.get(estab.municipio or "", "")

    # Natureza jurídica
    nat_desc = nat_map.get(empresa.natureza_juridica if empresa else "", "")
    qual_resp_desc = qual_map.get(empresa.qualificacao_responsavel if empresa else "", "")

    # Competências disponíveis
    competencias_disponiveis = sorted(
        Estabelecimento.objects.filter(cnpj_basico=cnpj_basico)
        .values_list("competencia", flat=True).distinct(),
        reverse=True,
    )

    return JsonResponse({
        "cnpj_basico": cnpj_basico,
        "cnpj": _format_cnpj(estab.cnpj_basico, estab.cnpj_ordem, estab.cnpj_dv),
        "competencia": competencia,
        "competencias_disponiveis": list(competencias_disponiveis),

        "razao_social": empresa.razao_social if empresa else "",
        "nome_fantasia": estab.nome_fantasia or "",
        "situacao": SITUACAO_LABEL.get(estab.situacao_cadastral or "", estab.situacao_cadastral or ""),
        "situacao_codigo": estab.situacao_cadastral or "",
        "data_situacao": _fmt_date(estab.data_situacao_cadastral),
        "data_abertura": _fmt_date(estab.data_inicio_atividade),
        "natureza_juridica": f"{empresa.natureza_juridica} - {nat_desc}" if empresa and nat_desc else (empresa.natureza_juridica if empresa else ""),
        "porte": PORTE_LABEL.get(empresa.porte if empresa else "", ""),
        "capital_social": empresa.capital_social if empresa else "",
        "qualificacao_responsavel": qual_resp_desc,
        "ente_federativo": empresa.ente_federativo_responsavel if empresa else "",

        "cnae_principal": {"codigo": cnae_principal, "descricao": cnae_principal_desc},
        "cnaes_secundarios": cnae_secundarios,

        "endereco": {
            "logradouro": f"{estab.tipo_logradouro or ''} {estab.logradouro or ''}".strip(),
            "numero": estab.numero or "",
            "complemento": estab.complemento or "",
            "bairro": estab.bairro or "",
            "municipio": municipio_desc,
            "municipio_codigo": estab.municipio or "",
            "uf": estab.uf or "",
            "cep": estab.cep or "",
        },

        "telefone": f"({estab.ddd1}) {estab.telefone1}" if estab.ddd1 and estab.telefone1 else "",
        "email": estab.correio_eletronico or "",
        "situacao_especial": estab.situacao_especial or "",

        "simples_nacional": {
            "optante": (simples_obj.opcao_simples or "") == "S" if simples_obj else False,
            "data_opcao": _fmt_date(simples_obj.data_opcao_simples) if simples_obj else "",
            "data_exclusao": _fmt_date(simples_obj.data_exclusao_simples) if simples_obj else "",
        },
        "mei": {
            "optante": (simples_obj.opcao_mei or "") == "S" if simples_obj else False,
            "data_opcao": _fmt_date(simples_obj.data_opcao_mei) if simples_obj else "",
            "data_exclusao": _fmt_date(simples_obj.data_exclusao_mei) if simples_obj else "",
        },

        "socios": socios,
    })
