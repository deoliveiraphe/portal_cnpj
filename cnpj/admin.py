from django.contrib import admin
from cnpj.models import (
    Cnae, Municipio, Pais, Natureza, Qualificacao, Motivo,
    Empresa, Estabelecimento, Socio, Simples, CargaLog,
)


@admin.register(Cnae)
class CnaeAdmin(admin.ModelAdmin):
    list_display = ["codigo", "descricao"]
    search_fields = ["codigo", "descricao"]


@admin.register(Municipio)
class MunicipioAdmin(admin.ModelAdmin):
    list_display = ["codigo", "descricao"]
    search_fields = ["descricao"]


@admin.register(Pais)
class PaisAdmin(admin.ModelAdmin):
    list_display = ["codigo", "descricao"]
    search_fields = ["descricao"]


@admin.register(Natureza)
class NaturezaAdmin(admin.ModelAdmin):
    list_display = ["codigo", "descricao"]
    search_fields = ["descricao"]


@admin.register(Qualificacao)
class QualificacaoAdmin(admin.ModelAdmin):
    list_display = ["codigo", "descricao"]


@admin.register(Motivo)
class MotivoAdmin(admin.ModelAdmin):
    list_display = ["codigo", "descricao"]


@admin.register(Empresa)
class EmpresaAdmin(admin.ModelAdmin):
    list_display = ["cnpj_basico", "razao_social", "porte", "competencia"]
    list_filter = ["competencia", "porte"]
    search_fields = ["cnpj_basico", "razao_social"]


@admin.register(Estabelecimento)
class EstabelecimentoAdmin(admin.ModelAdmin):
    list_display = ["cnpj_basico", "cnpj_ordem", "nome_fantasia", "situacao_cadastral", "uf", "competencia"]
    list_filter = ["competencia", "situacao_cadastral", "uf"]
    search_fields = ["cnpj_basico", "nome_fantasia"]


@admin.register(Socio)
class SocioAdmin(admin.ModelAdmin):
    list_display = ["cnpj_basico", "nome_socio", "identificador_socio", "qualificacao_socio", "competencia"]
    list_filter = ["competencia"]
    search_fields = ["cnpj_basico", "nome_socio"]


@admin.register(Simples)
class SimplesAdmin(admin.ModelAdmin):
    list_display = ["cnpj_basico", "opcao_simples", "opcao_mei", "competencia"]
    list_filter = ["competencia", "opcao_simples", "opcao_mei"]
    search_fields = ["cnpj_basico"]


@admin.register(CargaLog)
class CargaLogAdmin(admin.ModelAdmin):
    list_display = ["arquivo", "competencia", "qtd_registros", "status", "inicio", "fim"]
    list_filter = ["status", "competencia"]
    search_fields = ["arquivo"]
    readonly_fields = ["inicio", "fim"]
