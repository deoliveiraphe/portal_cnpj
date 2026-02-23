# Diagrama e Modelagem do Banco de Dados

Toda extração flui pelas tabelas parametrizadas do Django, mantendo versionamento em massa focado no mês em que a informação cadastral foi disponibilizada (A chamada `Competência`).

## Entidades Fixas (Domínio)
Tabelas menores sem acompanhamento de histórico / variação mensal. Toda vez que uma Carga flui no pipeline, essas tabelas são esvaziadas (truncate) e recarregadas.
* `cnpj_cnae`: Catalógo principal dos números de atividades de comércio. (~1.300 registros)
* `cnpj_municipio`: Dicionário geográfico de munícipios brasileiros por código IBGE/TSE.
* `cnpj_pais`, `cnpj_natureza`, `cnpj_qualificacao`, `cnpj_motivo`.

## Tabelas Principais (Entidades)
Agrupam dezenas de milhões de itens, contínuamente incrementadas e retendo dados passados graças ao controle primário pela coluna `competencia`.

```mermaid
erDiagram
    EMPRESA {
        string cnpj_basico FK "Chave Raíz sociétária (8 dígitos)"
        string razao_social
        string porte
        string capital_social
        string competencia "A qual mês de base este log pertence"
    }

    ESTABELECIMENTO {
        string cnpj_basico "Prefixo (8) - INDEXADO"
        string cnpj_ordem "Sufixo (4)"
        string cnpj_dv "Digito V. (2)"
        string identificador_matriz_filial
        string situacao_cadastral
        string municipio FK
        string cnae_fiscal_principal FK
        string logradouro
        string bairo
        string cep 
        string uf
        string competencia "INDEXADO"
    }

    SOCIO {
        string cnpj_basico
        string nome_socio
        string cnpj_cpf_socio "Com máscara"
        string faixa_etaria
        string competencia
    }

    SIMPLES {
        string cnpj_basico
        string opcao_simples "S ou N"
        string opcao_mei "S ou N"
        string competencia
    }

    EMPRESA ||--o{ ESTABELECIMENTO : "CNPJ Básico igual"
    EMPRESA ||--o{ SOCIO : "CNPJ Básico igual"
    EMPRESA ||--o| SIMPLES : "CNPJ Básico igual"
```

> [!NOTE]
> Observe no mapa relacional acima que **não existem `ForeignKeys`** físicas na base em colunas chaves (apenas indexações B-tree). Evitamos FKs reais para anular qualquer impacto de travamento (Locks) de cascata ao rodar operações de limpeza de `CASCADE constraints` durante os ETLs. A junção dessas chaves nas visualizações de tela é realizada abstraindo o número do `CNPJ Básico` do registro em runtime no ORM.
