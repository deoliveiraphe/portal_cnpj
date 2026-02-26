import time
import json
from django.shortcuts import render, redirect
from django.core.paginator import Paginator
from django.db.models import Subquery

from .models import (
    Empresa, Estabelecimento, Socio, Simples, Cnae, Municipio, CargaLog
)
from .views import (
    _latest_competencia, SITUACAO_LABEL, PORTE_LABEL,
    IDENTIFICADOR_SOCIO_LABEL, FAIXA_ETARIA_LABEL, _format_cnpj
)

PAGE_SIZE = 25

def home(request):
    competencias = sorted(
        Estabelecimento.objects.values_list("competencia", flat=True).distinct(),
        reverse=True,
    )
    ultima = competencias[0] if competencias else None
    total_empresas = Estabelecimento.objects.filter(competencia=ultima).count() if ultima else 0
    total_cargas = CargaLog.objects.filter(status="SUCESSO").count()

    return render(request, "cnpj/home.html", {
        "competencias": competencias,
        "total_empresas": total_empresas,
        "total_cargas": total_cargas,
    })

def busca(request):
    t0 = time.time()
    competencias = sorted(Estabelecimento.objects.values_list("competencia", flat=True).distinct(), reverse=True)
    competencia = request.GET.get("competencia") or _latest_competencia()

    if not request.GET:
        # Se não há filtros, não roda a query gigante, só renderiza a página de busca limpa (opcional)
        return render(request, "cnpj/busca.html", {
            "competencias": competencias,
            "competencia": competencia,
            "situacao_choices": SITUACAO_LABEL.items(),
            "porte_choices": PORTE_LABEL.items(),
        })

    q = request.GET.get("cnpj", "").strip() or request.GET.get("q", "").strip()
    razao_social = request.GET.get("razao_social", "").strip()
    cnae = request.GET.get("cnae_principal", "").strip()
    municipio = request.GET.get("municipio", "").strip()
    uf = request.GET.get("uf", "").strip().upper()
    situacao = request.GET.get("situacao_cadastral", "").strip()
    porte = request.GET.get("porte", "").strip()
    simples = request.GET.get("simples", "").strip().upper()
    mei = request.GET.get("mei", "").strip().upper()

    qs = Estabelecimento.objects.filter(competencia=competencia)

    if q:
        cnpj_limpo = ''.join(filter(str.isdigit, q))
        if cnpj_limpo:
            qs = qs.filter(cnpj_basico__startswith=cnpj_limpo[:8])
        else:
            qs = qs.filter(nome_fantasia__icontains=q)
    
    if razao_social:
        empresas = Empresa.objects.filter(competencia=competencia, razao_social__icontains=razao_social).values('cnpj_basico')
        qs = qs.filter(cnpj_basico__in=Subquery(empresas))
    
    if cnae:
        qs = qs.filter(cnae_fiscal_principal__startswith=cnae[:7])
    if uf:
        qs = qs.filter(uf=uf)
    if municipio:
        qs = qs.filter(municipio=municipio)
    if situacao:
        qs = qs.filter(situacao_cadastral=situacao)
        
    if porte:
        empresas = Empresa.objects.filter(competencia=competencia, porte=porte).values('cnpj_basico')
        qs = qs.filter(cnpj_basico__in=Subquery(empresas))
        
    if simples in ["S", "N"] or mei in ["S", "N"]:
        simples_qs = Simples.objects.filter(competencia=competencia)
        if simples in ["S", "N"]:
            simples_qs = simples_qs.filter(opcao_simples=simples)
        if mei in ["S", "N"]:
            simples_qs = simples_qs.filter(opcao_mei=mei)
        qs = qs.filter(cnpj_basico__in=Subquery(simples_qs.values('cnpj_basico')))

    qs = qs.order_by("cnpj_basico", "cnpj_ordem")

    paginator = Paginator(qs, PAGE_SIZE)
    page_number = request.GET.get("page", 1)
    
    try:
        page_obj = paginator.get_page(page_number)
    except Exception:
        page_obj = paginator.get_page(1)

    estab_list = list(page_obj.object_list)
    cnpj_basicos = [e.cnpj_basico for e in estab_list]
    
    empresas_map = {e.cnpj_basico: e for e in Empresa.objects.filter(competencia=competencia, cnpj_basico__in=cnpj_basicos)}
    cnae_map = dict(Cnae.objects.filter(codigo__in=[e.cnae_fiscal_principal for e in estab_list]).values_list("codigo", "descricao"))
    mun_map = dict(Municipio.objects.filter(codigo__in=[e.municipio for e in estab_list]).values_list("codigo", "descricao"))

    for e in estab_list:
        e.cnpj_complexo = _format_cnpj(e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv)
        e.situacao_descricao = SITUACAO_LABEL.get(e.situacao_cadastral, e.situacao_cadastral)
        if e.cnpj_basico in empresas_map:
            empresas_map[e.cnpj_basico].porte_descricao = PORTE_LABEL.get(empresas_map[e.cnpj_basico].porte, "")

    elapsed = round(time.time() - t0, 3)

    return render(request, "cnpj/busca.html", {
        "page_obj": page_obj,
        "total": paginator.count if request.GET else 0,
        "elapsed": elapsed,
        "filtros": request.GET,
        "competencia": competencia,
        "competencias": competencias,
        "situacao_choices": SITUACAO_LABEL.items(),
        "porte_choices": PORTE_LABEL.items(),
        "empresas_map": empresas_map,
        "cnae_map": cnae_map,
        "mun_map": mun_map,
    })

def detalhe(request, cnpj_basico):
    cnpj_basico = cnpj_basico.replace(".", "").replace("/", "").replace("-", "").zfill(8)
    competencia = request.GET.get("competencia") or _latest_competencia()

    estabelecimentos = Estabelecimento.objects.filter(cnpj_basico=cnpj_basico, competencia=competencia).order_by("cnpj_ordem")
    empresa = Empresa.objects.filter(cnpj_basico=cnpj_basico, competencia=competencia).first()
    
    if not estabelecimentos.exists() and not empresa:
        return render(request, "cnpj/detalhe.html", {
            "cnpj_basico": cnpj_basico, 
            "competencia": competencia,
            "error": "CNPJ não encontrado."
        })

    for e in estabelecimentos:
        e.cnpj_formatado = _format_cnpj(e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv)
        e.situacao_descricao = SITUACAO_LABEL.get(e.situacao_cadastral, e.situacao_cadastral)
        e.endereco_completo = f"{e.tipo_logradouro or ''} {e.logradouro or ''}, {e.numero or ''}".strip()
        if e.complemento: e.endereco_completo += f" - {e.complemento}"
        if e.bairro: e.endereco_completo += f", {e.bairro}"
        if e.cep: e.endereco_completo += f" - CEP: {e.cep}"

    socios = Socio.objects.filter(cnpj_basico=cnpj_basico, competencia=competencia)
    for s in socios:
        s.faixa_etaria_descricao = FAIXA_ETARIA_LABEL.get(s.faixa_etaria, "")

    simples = Simples.objects.filter(cnpj_basico=cnpj_basico, competencia=competencia).first()

    competencias = sorted(Estabelecimento.objects.filter(cnpj_basico=cnpj_basico).values_list("competencia", flat=True).distinct(), reverse=True)

    if empresa:
        empresa.porte_descricao = PORTE_LABEL.get(empresa.porte, "")

    cnae_principal_desc = ""
    if estabelecimentos.exists() and estabelecimentos.first().cnae_fiscal_principal:
        cnae_obj = Cnae.objects.filter(codigo=estabelecimentos.first().cnae_fiscal_principal).first()
        if cnae_obj:
            cnae_principal_desc = cnae_obj.descricao

    mun_desc = ""
    if estabelecimentos.exists() and estabelecimentos.first().municipio:
        m_obj = Municipio.objects.filter(codigo=estabelecimentos.first().municipio).first()
        if m_obj:
            mun_desc = m_obj.descricao

    # Histórico de situação (Matriz) para o chart.js
    hist = Estabelecimento.objects.filter(cnpj_basico=cnpj_basico, cnpj_ordem="0001").order_by("competencia")
    historico_labels = json.dumps([h.competencia for h in hist])
    historico_data = json.dumps([int(h.situacao_cadastral) if h.situacao_cadastral and h.situacao_cadastral.isdigit() else 0 for h in hist])

    return render(request, "cnpj/detalhe.html", {
        "cnpj_basico": cnpj_basico,
        "competencia": competencia,
        "competencias": competencias,
        "competencias_cnpj": competencias,
        "empresa": empresa,
        "estabelecimentos": estabelecimentos,
        "socios": socios,
        "simples": simples,
        "cnae_principal_desc": cnae_principal_desc,
        "municipio_desc": mun_desc,
        "historico_labels": historico_labels,
        "historico_data": historico_data,
    })
