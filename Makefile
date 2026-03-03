.PHONY: help up down logs build lint test clean load-lite shell psql migrate shell-db format

# Cores para o terminal
CYAN := \033[36m
RESET := \033[0m

help: ## Exibe a lista de comandos disponíveis
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "$(CYAN)make %-15s$(RESET) %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ── Docker ────────────────────────────────────────────────────────────────
up: ## Sobe a infraestrutura (Django, PG, Adminer, Elasticsearch) em background
	docker compose up -d

build: ## Reconstrói a imagem Docker do Django (necessário ao alterar requirements.txt)
	docker compose build django

down: ## Para todos os containers do projeto
	docker compose down

logs: ## Abre o tail (-f) dos logs gerais do Docker
	docker compose logs -f

# ── Qualidade e Testes ────────────────────────────────────────────────────
lint: ## Executa checagem de estilo de código usando Ruff
	docker compose exec django ruff check .
	docker compose exec django ruff format --check .

format: ## Aplica correção automática de formatação (Ruff + isort)
	docker compose exec django ruff check --fix .
	docker compose exec django ruff format .

test: ## Executa a bateria de testes usando Pytest
	docker compose exec django pytest

# ── Pipeline de Dados ─────────────────────────────────────────────────────
load-lite: ## Baixa dados recentes (lite), carrega no PG e indexa no Elasticsearch
	@echo "$(CYAN)1. Baixando Arquivos Lite...$(RESET)"
	docker compose exec django python manage.py download_cnpj --only-latest --lite
	@echo "$(CYAN)\n2. Carregando no PostgreSQL...$(RESET)"
	docker compose exec django python manage.py load_cnpj --lite --replace --competencia $$(date +%Y-%m -d "1 month ago")
	@echo "$(CYAN)\n3. Indexando no Elasticsearch...$(RESET)"
	docker compose exec django python manage.py index_es --create-index --workers 4 --competencia $$(date +%Y-%m -d "1 month ago")
	@echo "$(CYAN)\nCarga Lite Concluída!$(RESET)"

# ── Atalhos Django / DB ───────────────────────────────────────────────────
migrate: ## Roda as migrações do banco de dados (makemigrations e migrate)
	docker compose exec django python manage.py makemigrations
	docker compose exec django python manage.py migrate

shell: ## Abre o Shell interativo do Django
	docker compose exec django python manage.py shell

psql: ## Abre o terminal do PostgreSQL (via container dbs)
	docker compose exec postgres psql -U cnpj -d cnpj

clean: ## Limpa pycache e artefatos de build do projeto local
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.log" -delete
