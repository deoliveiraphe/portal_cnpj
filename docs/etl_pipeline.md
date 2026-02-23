# O Pipeline ETL (Download & Carga)

A automação da leitura dos dados não demanda ferramentas externas pesadas (como Apache Airflow uasado comumente). Tudo está encapsulado em comandos gerenciais (*Management Commands*) do próprio pacote do Django.

## Fase 1: Ingestão HTTP (Download)

O processo de coleta busca as publicações mensais (identificadas como **Competências**, padrão ISO de mês `YYYY-MM`) localizadas na base federal.

* **Comando:** `python manage.py download_cnpj`
* **Principais Funcionalidades:**
    - Verifica o índice HTML cru da Receita apontando os diretórios do servidor deles.
    - Suporta flags atitudinais como baixar apenas "última" (`--only-latest`) para testes locais, ou de datas programáticas exclusivas (`--start` / `--end`).
    - Possui mecanismo de auto-retry com *backoff logarítmico*. Falhas na conexão de rede governamental retomam sem interromper todo o workflow da noite.
    - Ignora o re-download desnecessário se a contagem de bytes já bater com os parciais presentes no storage em `data/raw/YYYY-MM/`.

## Fase 2: Tratamento, Transformação e Carga (ETL)

Arquivos brutos baixados não podem ser colocados num banco SQL impunemente devido ao volume massivo. O Django ORM executando a submissão via metódo `.create()` linha-a-linha iria falhar com estouro de memória ou *Connection Timeout*.

* **Comando:** `python manage.py load_cnpj`
* **O Fluxo passo a passo:**
    1. A rotina não descompacta o arquivo no servidor para economizar File System (`storage I/O`). A biblioteca ZipFile do python intercepta e injeta bytes abertos.
    2. Lê em "pedacinhos" de 100 mil registros (`chunksize=100_000`) cada CSV presente no pacote daquela Competência.
    3. Trata lixos conhecidos do formatao do governo:
       - Transforma datas estranhas como `00000000` em um formato NULL aceitável para colunas `DATE` no painel do banco de dados.
       - Mascara partes dos CPFs de sócios da companhia resguardando LGPD parcialmente conforme imposto nas novas coletas do Ministério da Fazenda.
       - Aplica trim(strip) padronizado removendo espaços sujos dos limites das Strings.
       - Substituí valores inexistentes do pandas (`NaN`) pela literal Python `None`.
    4. Usa Injeção `COPY from stdin`. O Psycopg2 recebe os pedaços tratados e despeja sem travas de parser ANSI-SQL no postgresquel. Essa abordagem é mais de 50x mais rápida do que Bulk Inserts tradicionais com queries preparadas.
    5. No fim das consolidações das dez particões (`Empresas0.zip` até `Empresas9.zip`), é registrado o resultado em uma tabela de Auditoria em tela chamada de `Log de Cargas` (`cnpj_carga_log`).
