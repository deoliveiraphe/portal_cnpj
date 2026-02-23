"""
Models do app cnpj.

Tabelas de domínio: sem campo competencia, substituídas integralmente a cada carga.
Tabelas principais: possuem campo competencia (YYYY-MM) p/ manutenção do histórico.

Todos os campos de código são CharField para preservar zeros à esquerda.
"""
from django.db import models
from django.contrib.postgres.indexes import GinIndex


# ─────────────────────────────────────────────
# TABELAS DE DOMÍNIO
# ─────────────────────────────────────────────

class Cnae(models.Model):
    codigo = models.CharField("Código", max_length=7, primary_key=True)
    descricao = models.CharField("Descrição", max_length=255)

    class Meta:
        db_table = "cnpj_cnae"
        verbose_name = "CNAE"
        verbose_name_plural = "CNAEs"
        ordering = ["codigo"]

    def __str__(self):
        return f"{self.codigo} — {self.descricao}"


class Municipio(models.Model):
    codigo = models.CharField("Código", max_length=7, primary_key=True)
    descricao = models.CharField("Descrição", max_length=60)

    class Meta:
        db_table = "cnpj_municipio"
        verbose_name = "Município"
        verbose_name_plural = "Municípios"
        ordering = ["descricao"]

    def __str__(self):
        return f"{self.descricao} ({self.codigo})"


class Pais(models.Model):
    codigo = models.CharField("Código", max_length=3, primary_key=True)
    descricao = models.CharField("Descrição", max_length=70)

    class Meta:
        db_table = "cnpj_pais"
        verbose_name = "País"
        verbose_name_plural = "Países"
        ordering = ["descricao"]

    def __str__(self):
        return f"{self.descricao} ({self.codigo})"


class Natureza(models.Model):
    codigo = models.CharField("Código", max_length=4, primary_key=True)
    descricao = models.CharField("Descrição", max_length=100)

    class Meta:
        db_table = "cnpj_natureza"
        verbose_name = "Natureza Jurídica"
        verbose_name_plural = "Naturezas Jurídicas"
        ordering = ["codigo"]

    def __str__(self):
        return f"{self.codigo} — {self.descricao}"


class Qualificacao(models.Model):
    codigo = models.CharField("Código", max_length=2, primary_key=True)
    descricao = models.CharField("Descrição", max_length=100)

    class Meta:
        db_table = "cnpj_qualificacao"
        verbose_name = "Qualificação"
        verbose_name_plural = "Qualificações"
        ordering = ["codigo"]

    def __str__(self):
        return f"{self.codigo} — {self.descricao}"


class Motivo(models.Model):
    codigo = models.CharField("Código", max_length=2, primary_key=True)
    descricao = models.CharField("Descrição", max_length=100)

    class Meta:
        db_table = "cnpj_motivo"
        verbose_name = "Motivo de Situação Cadastral"
        verbose_name_plural = "Motivos de Situação Cadastral"
        ordering = ["codigo"]

    def __str__(self):
        return f"{self.codigo} — {self.descricao}"


# ─────────────────────────────────────────────
# TABELAS PRINCIPAIS (com competencia)
# ─────────────────────────────────────────────

class Empresa(models.Model):
    cnpj_basico = models.CharField("CNPJ Básico", max_length=8, db_index=True)
    razao_social = models.CharField("Razão Social", max_length=150, blank=True, null=True)
    natureza_juridica = models.CharField("Natureza Jurídica", max_length=4, blank=True, null=True)
    qualificacao_responsavel = models.CharField("Qualificação do Responsável", max_length=2, blank=True, null=True)
    capital_social = models.CharField("Capital Social", max_length=20, blank=True, null=True)
    porte = models.CharField("Porte", max_length=2, blank=True, null=True)
    ente_federativo_responsavel = models.CharField("Ente Federativo Responsável", max_length=50, blank=True, null=True)
    competencia = models.CharField("Competência", max_length=7, db_index=True)

    class Meta:
        db_table = "cnpj_empresa"
        verbose_name = "Empresa"
        verbose_name_plural = "Empresas"
        indexes = [
            models.Index(fields=["cnpj_basico", "competencia"], name="idx_empresa_cnpj_comp"),
            GinIndex(fields=["razao_social"], name="idx_empresa_razao_gin", opclasses=["gin_trgm_ops"]),
        ]

    def __str__(self):
        return f"{self.cnpj_basico} — {self.razao_social} ({self.competencia})"

    @property
    def porte_descricao(self):
        mapa = {
            "00": "Não Informado",
            "01": "Micro Empresa",
            "03": "Empresa de Pequeno Porte",
            "05": "Demais",
        }
        return mapa.get(self.porte, self.porte or "Não Informado")


class Estabelecimento(models.Model):
    cnpj_basico = models.CharField("CNPJ Básico", max_length=8, db_index=True)
    cnpj_ordem = models.CharField("CNPJ Ordem", max_length=4)
    cnpj_dv = models.CharField("CNPJ DV", max_length=2)
    identificador_matriz_filial = models.CharField("Matriz/Filial", max_length=1, blank=True, null=True)
    nome_fantasia = models.CharField("Nome Fantasia", max_length=55, blank=True, null=True)
    situacao_cadastral = models.CharField("Situação Cadastral", max_length=2, blank=True, null=True, db_index=True)
    data_situacao_cadastral = models.DateField("Data Situação Cadastral", blank=True, null=True)
    motivo_situacao_cadastral = models.CharField("Motivo Situação Cadastral", max_length=2, blank=True, null=True)
    nome_cidade_exterior = models.CharField("Nome Cidade Exterior", max_length=55, blank=True, null=True)
    pais = models.CharField("País", max_length=3, blank=True, null=True)
    data_inicio_atividade = models.DateField("Data Início Atividade", blank=True, null=True)
    cnae_fiscal_principal = models.CharField("CNAE Fiscal Principal", max_length=7, blank=True, null=True, db_index=True)
    cnae_fiscal_secundaria = models.TextField("CNAEs Secundários", blank=True, null=True)
    tipo_logradouro = models.CharField("Tipo Logradouro", max_length=20, blank=True, null=True)
    logradouro = models.CharField("Logradouro", max_length=60, blank=True, null=True)
    numero = models.CharField("Número", max_length=6, blank=True, null=True)
    complemento = models.CharField("Complemento", max_length=156, blank=True, null=True)
    bairro = models.CharField("Bairro", max_length=50, blank=True, null=True)
    cep = models.CharField("CEP", max_length=8, blank=True, null=True)
    uf = models.CharField("UF", max_length=2, blank=True, null=True, db_index=True)
    municipio = models.CharField("Município (código)", max_length=7, blank=True, null=True, db_index=True)
    ddd1 = models.CharField("DDD 1", max_length=4, blank=True, null=True)
    telefone1 = models.CharField("Telefone 1", max_length=8, blank=True, null=True)
    ddd2 = models.CharField("DDD 2", max_length=4, blank=True, null=True)
    telefone2 = models.CharField("Telefone 2", max_length=8, blank=True, null=True)
    ddd_fax = models.CharField("DDD Fax", max_length=4, blank=True, null=True)
    fax = models.CharField("Fax", max_length=8, blank=True, null=True)
    correio_eletronico = models.CharField("E-mail", max_length=115, blank=True, null=True)
    situacao_especial = models.CharField("Situação Especial", max_length=100, blank=True, null=True)
    data_situacao_especial = models.DateField("Data Situação Especial", blank=True, null=True)
    competencia = models.CharField("Competência", max_length=7, db_index=True)

    class Meta:
        db_table = "cnpj_estabelecimento"
        verbose_name = "Estabelecimento"
        verbose_name_plural = "Estabelecimentos"
        indexes = [
            models.Index(fields=["cnpj_basico", "competencia"], name="idx_estab_cnpj_comp"),
            models.Index(fields=["uf", "municipio", "competencia"], name="idx_estab_uf_municipio"),
            models.Index(fields=["cnae_fiscal_principal", "competencia"], name="idx_estab_cnae_comp"),
            models.Index(fields=["situacao_cadastral", "competencia"], name="idx_estab_sit_comp"),
        ]

    def __str__(self):
        return f"{self.cnpj_basico}{self.cnpj_ordem}{self.cnpj_dv} ({self.competencia})"

    @property
    def cnpj_completo(self):
        return f"{self.cnpj_basico}{self.cnpj_ordem}{self.cnpj_dv}"

    @property
    def cnpj_formatado(self):
        c = self.cnpj_completo
        if len(c) == 14:
            return f"{c[:2]}.{c[2:5]}.{c[5:8]}/{c[8:12]}-{c[12:]}"
        return c

    @property
    def situacao_descricao(self):
        mapa = {
            "01": "NULA",
            "02": "ATIVA",
            "03": "SUSPENSA",
            "04": "INAPTA",
            "08": "BAIXADA",
        }
        return mapa.get(self.situacao_cadastral, self.situacao_cadastral or "—")

    @property
    def endereco_completo(self):
        partes = [
            f"{self.tipo_logradouro or ''} {self.logradouro or ''}".strip(),
            self.numero,
            self.complemento,
            self.bairro,
            self.cep,
            self.uf,
        ]
        return ", ".join(p for p in partes if p)


class Socio(models.Model):
    cnpj_basico = models.CharField("CNPJ Básico", max_length=8, db_index=True)
    identificador_socio = models.CharField("Identificador do Sócio", max_length=1, blank=True, null=True)
    nome_socio = models.CharField("Nome do Sócio", max_length=150, blank=True, null=True)
    cnpj_cpf_socio = models.CharField("CNPJ/CPF do Sócio (mascarado)", max_length=14, blank=True, null=True)
    qualificacao_socio = models.CharField("Qualificação do Sócio", max_length=2, blank=True, null=True)
    data_entrada_sociedade = models.DateField("Data Entrada na Sociedade", blank=True, null=True)
    pais = models.CharField("País", max_length=3, blank=True, null=True)
    representante_legal = models.CharField("CPF Representante Legal", max_length=11, blank=True, null=True)
    nome_representante = models.CharField("Nome do Representante", max_length=60, blank=True, null=True)
    qualificacao_representante = models.CharField("Qualificação Representante", max_length=2, blank=True, null=True)
    faixa_etaria = models.CharField("Faixa Etária", max_length=1, blank=True, null=True)
    competencia = models.CharField("Competência", max_length=7, db_index=True)

    class Meta:
        db_table = "cnpj_socio"
        verbose_name = "Sócio"
        verbose_name_plural = "Sócios"
        indexes = [
            models.Index(fields=["cnpj_basico", "competencia"], name="idx_socio_cnpj_comp"),
        ]

    def __str__(self):
        return f"{self.nome_socio} ({self.cnpj_basico} / {self.competencia})"

    @property
    def faixa_etaria_descricao(self):
        mapa = {
            "1": "0-12 anos",
            "2": "13-20 anos",
            "3": "21-30 anos",
            "4": "31-40 anos",
            "5": "41-50 anos",
            "6": "51-60 anos",
            "7": "61-70 anos",
            "8": "71-80 anos",
            "9": "81 anos ou mais",
            "0": "Não informado",
        }
        return mapa.get(self.faixa_etaria, "—")


class Simples(models.Model):
    cnpj_basico = models.CharField("CNPJ Básico", max_length=8, db_index=True)
    opcao_simples = models.CharField("Opção Simples", max_length=1, blank=True, null=True)
    data_opcao_simples = models.DateField("Data Opção Simples", blank=True, null=True)
    data_exclusao_simples = models.DateField("Data Exclusão Simples", blank=True, null=True)
    opcao_mei = models.CharField("Opção MEI", max_length=1, blank=True, null=True)
    data_opcao_mei = models.DateField("Data Opção MEI", blank=True, null=True)
    data_exclusao_mei = models.DateField("Data Exclusão MEI", blank=True, null=True)
    competencia = models.CharField("Competência", max_length=7, db_index=True)

    class Meta:
        db_table = "cnpj_simples"
        verbose_name = "Simples Nacional / MEI"
        verbose_name_plural = "Simples Nacional / MEI"
        indexes = [
            models.Index(fields=["cnpj_basico", "competencia"], name="idx_simples_cnpj_comp"),
            models.Index(fields=["opcao_simples", "opcao_mei", "competencia"], name="idx_simples_opcoes_comp"),
        ]

    def __str__(self):
        return f"{self.cnpj_basico} — Simples:{self.opcao_simples} MEI:{self.opcao_mei} ({self.competencia})"


# ─────────────────────────────────────────────
# AUDITORIA
# ─────────────────────────────────────────────

class CargaLog(models.Model):
    STATUS_CHOICES = [
        ("INICIADO", "Iniciado"),
        ("SUCESSO", "Sucesso"),
        ("ERRO", "Erro"),
        ("PARCIAL", "Parcial"),
    ]

    arquivo = models.CharField("Arquivo", max_length=80)
    competencia = models.CharField("Competência", max_length=7, db_index=True)
    qtd_registros = models.BigIntegerField("Qtd. Registros", default=0)
    status = models.CharField("Status", max_length=10, choices=STATUS_CHOICES, default="INICIADO")
    inicio = models.DateTimeField("Início", auto_now_add=True)
    fim = models.DateTimeField("Fim", blank=True, null=True)
    erro = models.TextField("Mensagem de Erro", blank=True, null=True)

    class Meta:
        db_table = "cnpj_carga_log"
        verbose_name = "Log de Carga"
        verbose_name_plural = "Logs de Carga"
        ordering = ["-inicio"]

    def __str__(self):
        return f"{self.arquivo} | {self.competencia} | {self.status}"
