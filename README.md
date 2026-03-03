# Portal de Dados Abertos CNPJ — Receita Federal do Brasil

Portal de consulta ao Cadastro Nacional de Pessoas Jurídicas (CNPJ) oferecendo uma solução completa: pipeline de download da RFB, processamento ETL paralelo robusto (PostgreSQL) e busca textual rápida (Elasticsearch). Acompanha interface web moderna (dark navy) e API RESTful.

---

## 🏗️ Arquitetura

O sistema emprega uma arquitetura baseada em contêineres e banco de dados relacional para persistência master, utilizando um motor de busca secundário para pesquisas rápidas.

```text
┌─────────────────────────────────────────────┐
│              Docker Compose                 │
│                                             │
│  ┌──────────┐  ┌──────────┐  ┌───────────┐  │
│  │ postgres │  │  django  │  │  adminer  │  │
│  │  :5432   │  │  :8000   │  │   :8081   │  │
│  └──────────┘  └──────────┘  └───────────┘  │
│                                             │
│  ┌──────────────────┐                       │
│  │ elasticsearch    │                       │
│  │      :9200       │                       │
│  └──────────────────┘                       │
│                                             │
│  Volumes: pgdata, esdata, ./data, ./logs    │
└─────────────────────────────────────────────┘
```

### O Pipeline (ETL)

1. **Download**: Receita Federal → `download_cnpj.py` → `data/raw/YYYY-MM/*.zip`
2. **Carga**: `data/raw/YYYY-MM/*.zip` → `load_cnpj.py` (Workers Paralelos) → **PostgreSQL 15**
3. **Indexação**: PostgreSQL 15 → `index_es.py` (Bulk) → **Elasticsearch 8**
4. **Consulta**: Django `api_busca` → Frontend / API REST

### Tecnologias

| Camada | Tecnologia |
|--------|-----------|
| **Backend** | Django 4.2 |
| **Banco Relacional** | PostgreSQL 15 |
| **Busca Textual** | Elasticsearch 8 + `django-elasticsearch-dsl` |
| **ETL (Carga)** | Python + pandas + psycopg2 (`COPY`) + `ProcessPoolExecutor` |
| **ETL (Indexação)**| `elasticsearch-py` `bulk()` + Cursor Raw Django |
| **Extração** | `requests` + `tqdm` |
| **Frontend** | Bootstrap 5 + Chart.js |
| **Infraestrutura**| Docker Compose |

---

## 🚀 Pré-requisitos

- Docker e Docker Compose v2+
- **Disco**: 
  - `~80–100 GB` para base completa (1 competência inteira).
  - `~3–5 GB` no **Modo Lite** (ideal para testes ou ambientes limitados).
- Internet estável para o download de dados da Receita Federal.

---

## 🛠️ Guia de Instalação e Uso

### 1. Preparando o Ambiente

Clone o repositório, configure as variáveis de ambiente baseadas no exemplo e inicie os contêineres em *background*:

```bash
git clone <repo> portal_cnpj
cd portal_cnpj
cp .env.example .env

docker compose up -d
```
Aguarde alguns instantes para o banco de dados inicializar e as migrações do Django ocorrerem de forma automática.

### 2. O Pipeline de Dados

Para popular o portal, você executará as 3 etapas essenciais do pipeline. Caso seja seu primeiro uso ou você esteja apenas testando localmente, **recomendamos iniciar pelo Modo Lite**.

> 💡 **MODO LITE**: O projeto oferece suporte a um modo econômico, perfeito para testes locais limitados de espaço. Use as flags `--lite` ao rodar os comandos para baixar/processar apenas cerca de ~10% dos dados.

#### Etapa A: Download dos Arquivos

Baixe os dados processados publicamente pela Receita Federal.

```bash
# Baixa apenas os dados mais recentes (Mês Atual/Anterior)
# Adicione --lite para um download super rápido (Testes)
docker compose exec django python manage.py download_cnpj --only-latest --lite

# Controle Fino (exemplo: pula tabelas secundárias):
docker compose exec django python manage.py download_cnpj --only-latest --slices 2 --skip-tables socio simples
```

#### Etapa B: Carga no PostgreSQL

Converte de ZIP via streaming e insere paralelamente no banco via protocolo veloz (`COPY`).

```bash
# Atalho --lite: carrega apenas as primeiras fatias e ignora as pesadas tabelas de Simples Nacional
docker compose exec django python manage.py load_cnpj --competencia 2026-02 --lite --replace

# Carga COMPLETA (todas as competências e fatias). Ajuste os --workers de acordo com sua CPU (teste com nproc):
docker compose exec django python manage.py load_cnpj --all --workers 8
```
*Dica:* Ao rodar `load_cnpj`, o terminal exibirá instruções para acompanhar o **arquivo de log detalhado em tempo real** nos hosts.

#### Etapa C: Indexação Rápida no Elasticsearch

Por fim, povoe o índice textual para que a busca funcione instantaneamente. O comando lê do Postgres em blocos com baixo consumo de memória (RAM) enviando *Bulks* ao serviço do Elastic.

```bash
# Cria o índice ES e popula com os dados (Funciona 100% igual no modo lite)
docker compose exec django python manage.py index_es --competencia 2026-02 --create-index

# Apenas reindexar futuramente:
docker compose exec django python manage.py index_es --competencia 2026-02 --replace
```
*Dica:* Siga os logs sugeridos pelo terminal (`tail -f`) ou verifique as contagens finais: `curl -s http://localhost:9200/cnpj_estabelecimentos/_count`.

<details>
<summary><b>👀 Estimativa de Economia do Modo Lite</b></summary>
<br>

| Modo | ZIPs baixados | Registros (~) | Espaço DB |
|------|:---:|:---:|:---:|
| Completo | 33 | ~163 M | ~80–100 GB |
| `--slices 1` | 9 | ~16 M | ~8–10 GB |
| `--lite` (Atalho param.)| 9 | ~14 M | ~7–9 GB |
| `--slices 1 --skip-tables simples socio` | 8 | ~6 M | ~3–4 GB |

O parâmetro `--slices N` indica que apenas os X primeiros arquivos de cada tabela volumétrica foram extraídos (Empresas, Estab., Sócios). Tabelas cruciais de domínio (CNAE, Municípios) são trazidas com integridade total.
</details>

### 3. Acessando a Solução

Após carga concluída, seu Portal CNPJ está funcional. Acesse:

| Serviço / Página | URL de Acesso |
|------------------|---------------|
| **Portal de Busca (Home)** | `http://localhost:8000/` |
| **Busca Avançada Flexível**| `http://localhost:8000/busca/` |
| **Detalhe de Perfil CNPJ** | `http://localhost:8000/cnpj/<cnpj_basico>/` |
| **Endpoints REST Publicos**| `http://localhost:8000/api/cnpj/<cnpj>/` |
| Painel do Servidor Django  | `http://localhost:8000/admin/` |
| Adminer DB Client (Postgres)| `http://localhost:8081/` |
| Status Health Elasticsearch| `http://localhost:9200/_cluster/health` |

---

## ⚙️ Variáveis de Ambiente (`.env`)

| Variável | Valor Padrão | Descrição |
|----------|--------------|-----------|
| `POSTGRES_DB` | `cnpj` | Nome do banco de dados |
| `POSTGRES_USER` | `cnpj` | Usuário de autenticação PostgreSQL |
| `POSTGRES_PASSWORD` | `cnpj123` | Senha padronizada de ambiente |
| `DJANGO_SECRET_KEY` | `dev-key` | Chave criptográfica Django |
| `DJANGO_DEBUG` | `False` | Alterar p/ `True` durante manutenção |
| `DJANGO_ALLOWED_HOSTS` | `localhost,127.0.0.1` | Segurança de *headers* web |
| `ES_URL` | `http://elasticsearch:9200` | URL do Elasticsearch para conexão interna |
| `CNPJ_ES_INDEX` | `cnpj_estabelecimentos` | Nome do *index* gerenciado pelo Elastic |

---

## 📚 Visão Interna: Tabelas do Banco de Dados

### Tabelas Padrão de Domínio (Lista Estática Censitária)
Tabelas indexadas de forma permanente (sem coluna de período de competência):

| Tabela Referência | O que Armazena | Num de Registros Aprox. |
|-------------------|----------------|-------------------------|
| `cnpj_cnae` | Atividades econômicas descritivas | ~1.300 |
| `cnpj_municipio` | Relação de siglas (RFB) e capitais | ~5.570 |
| `cnpj_pais` | Países/Nacionalidades | ~260 |
| `cnpj_natureza` | Natureza Jurídica formalizada | ~90 |
| `cnpj_qualificacao` | Qualificação societária (Ex: Presidente, Diretor) | ~70 |
| `cnpj_motivo` | Situações Censitárias Especiais | ~60 |

### Tabelas Mapeadas por Competência Mensal
Para o volume esmagador da Receita, guardamos isolamento mês a mês para auditoria temporal (`YYYY-MM`).

| Tabela Base | Descrição Funcional | Volumes por Carga Full |
|-------------|---------------------|------------------------|
| `cnpj_empresa` | Raiz Organizacional | ~60 milhões |
| `cnpj_estabelecimento`| Filiais Físicas (CNPJ 14 dígitos) | ~60 milhões |
| `cnpj_socio` | Relação quadro de pessoas / QSA | ~25 milhões |
| `cnpj_simples` | Assinatura Optantes do Simples / MEI | ~18 milhões |

---

## 💡 Comandos e Dicas de Operação

Verificar a quantidade limite de CPUs livres na sua máquina host local (útil para calibrar contagem de `--workers N` nos ETLs de processamento):
```bash
docker compose exec django nproc
```

Para verificar progressos de Cargas ETL logados internamente no banco de dados, inspecione as estatísticas via Shell interativo Django:
```bash
docker compose exec django python manage.py shell -c \
  "from cnpj.models import CargaLog; [print(l.arquivo, l.status, l.qtd_registros) for l in CargaLog.objects.all()[:10]]"
```

Validação do banco relacional - conte os volumes gigantes com agregação do Django:
```bash
docker compose exec django python manage.py shell -c \
  "from cnpj.models import Empresa; from django.db.models import Count; print(list(Empresa.objects.values('competencia').annotate(n=Count('id'))))"
```

Paradas seguras, mantendo as migrações e toda a carga massiva de disco preservada através dos *Named Volumes* do *Compose*:
```bash
docker compose down
```
*(Atenção: Apenas adicione `-v` ali se realmente desejar o destrutivo "purge" perdendo os dados.)*

---

> Dados públicos de licença aberta fornecidos com base documental pela **Receita Federal do Brasil**.  
> Arquitetura e código de software sob licença **MIT**, livre e reutilizável por qualquer fim comercial.
