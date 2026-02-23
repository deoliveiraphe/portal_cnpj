# Portal de Dados Abertos CNPJ — Receita Federal do Brasil

Portal de consulta ao Cadastro Nacional de Pessoas Jurídicas (CNPJ) com pipeline completo de download, processamento ETL e interface web moderna.

## Arquitetura

```
┌─────────────────────────────────────────────┐
│              Docker Compose                  │
│                                             │
│  ┌──────────┐  ┌──────────┐  ┌───────────┐ │
│  │ postgres │  │  django  │  │  adminer  │ │
│  │  :5432   │  │  :8000   │  │   :8080   │ │
│  └──────────┘  └──────────┘  └───────────┘ │
│                                             │
│  Volumes: pgdata, ./data, ./logs            │
└─────────────────────────────────────────────┘

Receita Federal → download_cnpj.py → data/raw/YYYY-MM/*.zip
                                          ↓
data/raw/YYYY-MM/*.zip → load_cnpj.py  → PostgreSQL 15
                                          ↓
                               Django ORM → Templates Bootstrap 5
```

## Tecnologias

| Camada | Tecnologia |
|--------|-----------|
| Backend | Django 4.2 + Django ORM |
| Banco | PostgreSQL 15 |
| ETL | Python + pandas + psycopg2 COPY |
| Download | requests + tqdm |
| Frontend | Bootstrap 5 + Chart.js |
| Infra | Docker Compose |

## Pré-requisitos

- Docker e Docker Compose v2+
- **200 GB de disco** para base completa (14 competências × ~40 arquivos)
- Conexão internet para download dos dados da Receita Federal

## Início rápido

### 1. Clone e configure

```bash
git clone <repo> mihos
cd mihos
cp .env.example .env
# Edite .env conforme necessário
```

### 2. Suba os containers

```bash
docker compose up -d
```

Aguarde os containers ficarem saudáveis (postgres + migrate acontecem automaticamente).

### 3. Faça o download dos dados

```bash
# Baixa apenas a competência mais recente (recomendado para teste)
docker compose exec django python manage.py download_cnpj --only-latest

# Baixa o intervalo completo 2025-01 → 2026-01 (pode levar horas!)
docker compose exec django python manage.py download_cnpj --start 2025-01 --end 2026-01

# Baixa uma competência específica
docker compose exec django python manage.py download_cnpj --start 2025-06 --end 2025-06
```

### 4. Carregue os dados no banco

```bash
# Carrega uma competência
docker compose exec django python manage.py load_cnpj --competencia 2025-06

# Carrega todas as competências disponíveis em data/raw/
docker compose exec django python manage.py load_cnpj --all

# Recarrega substituindo dados existentes
docker compose exec django python manage.py load_cnpj --competencia 2025-06 --replace
```

### 5. Acesse

| URL | Descrição |
|-----|-----------|
| `http://localhost:8000/` | Portal de busca |
| `http://localhost:8000/busca/` | Busca avançada |
| `http://localhost:8000/cnpj/<cnpj_basico>/` | Detalhe de empresa |
| `http://localhost:8000/api/cnpj/<cnpj>/` | API REST (JSON) |
| `http://localhost:8080/` | Adminer (PostgreSQL) |
| `http://localhost:8000/admin/` | Django Admin |

## Estrutura do Projeto

```
mihos/
├── cnpj_portal/           # Projeto Django
│   ├── settings.py        # Configurações (env vars)
│   └── urls.py
├── cnpj/                  # App principal
│   ├── management/commands/
│   │   ├── download_cnpj.py   # Etapa 1: download
│   │   └── load_cnpj.py       # Etapa 3: ETL
│   ├── models.py              # Etapa 2: modelos
│   ├── views.py               # Etapa 4: interface
│   └── templates/cnpj/
│       ├── base.html          # Layout base Bootstrap 5
│       ├── home.html          # Página inicial
│       ├── busca.html         # Busca com filtros
│       └── detalhe.html       # Detalhe + Chart.js
├── data/raw/              # ZIPs da RF (volume Docker)
│   └── 2025-06/
│       ├── Cnaes.zip
│       ├── Empresas0.zip ... Empresas9.zip
│       └── ...
├── logs/                  # Logs de download por competência
├── Dockerfile
├── docker-compose.yml
├── entrypoint.sh
└── requirements.txt
```

## Tabelas do Banco

### Domínio (sem histórico)
| Tabela | Registros aprox. |
|--------|-----------------|
| `cnpj_cnae` | ~1.300 |
| `cnpj_municipio` | ~5.570 |
| `cnpj_pais` | ~260 |
| `cnpj_natureza` | ~90 |
| `cnpj_qualificacao` | ~70 |
| `cnpj_motivo` | ~60 |

### Principais (com campo `competencia`)
| Tabela | Registros por competência |
|--------|--------------------------|
| `cnpj_empresa` | ~60 milhões |
| `cnpj_estabelecimento` | ~60 milhões |
| `cnpj_socio` | ~25 milhões |
| `cnpj_simples` | ~18 milhões |

## Funcionalidades

### Download (`download_cnpj`)
- ✅ Retry com backoff exponencial (3 tentativas)
- ✅ Skip de arquivos já baixados
- ✅ Barras de progresso por arquivo e competência
- ✅ Log de erros por competência em `./logs/`
- ✅ `--only-latest` para baixar só a mais recente

### ETL (`load_cnpj`)
- ✅ Extração ZIP em memória (sem escrever no disco)
- ✅ Leitura em chunks de 100.000 linhas (pandas)
- ✅ Layout oficial da RF aplicado automaticamente
- ✅ Parse de datas YYYYMMDD com tratamento de datas inválidas
- ✅ Mascaramento de CPF de sócios (`***456789**`)
- ✅ Normalização de strings (strip, upper, vazio→None)
- ✅ Carga via `COPY` (psycopg2) — máxima performance
- ✅ Registro de logs em `CargaLog`
- ✅ `--replace` para recarregar competência existente

### Interface
- ✅ Busca com 10 filtros (CNPJ, razão social, CNAE, município, UF, situação, porte, Simples, MEI)
- ✅ Paginação 25/página com contador de resultados e tempo de resposta
- ✅ Detalhe com tabs: Empresa, Estabelecimento, Sócios, Simples, Histórico
- ✅ Gráfico de evolução histórica da situação cadastral (Chart.js)
- ✅ Comparativo entre competências disponíveis
- ✅ API REST JSON em `/api/cnpj/<cnpj>/`
- ✅ Design dark premium com Bootstrap 5

## Variáveis de Ambiente

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `POSTGRES_DB` | `cnpj` | Nome do banco |
| `POSTGRES_USER` | `cnpj` | Usuário do banco |
| `POSTGRES_PASSWORD` | `cnpj123` | Senha do banco |
| `DJANGO_SECRET_KEY` | `dev-key` | Chave secreta Django |
| `DJANGO_DEBUG` | `False` | Modo debug |
| `DJANGO_ALLOWED_HOSTS` | `localhost,127.0.0.1` | Hosts permitidos |

## Comandos úteis

```bash
# Verificar logs de carga
docker compose exec django python manage.py shell -c \
  "from cnpj.models import CargaLog; [print(l) for l in CargaLog.objects.all()[:10]]"

# Contar registros por competência
docker compose exec django python manage.py shell -c \
  "from cnpj.models import Empresa; print(Empresa.objects.values('competencia').annotate(n=__import__('django.db.models',fromlist=['Count']).Count('id')))"

# Parar e remover tudo (preserva volume pgdata)
docker compose down

# Remover volume com dados (CUIDADO!)
docker compose down -v
```

## Licença

Dados públicos fornecidos pela Receita Federal do Brasil.  
Código sob licença MIT.
