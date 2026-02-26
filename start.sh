#!/bin/bash
set -e

echo "=========================================="
echo "    Iniciando Portal de Dados CNPJ        "
echo "=========================================="

echo "[1/3] Verificando arquivo .env..."
if [ ! -f ".env" ]; then
    if [ -f ".env.example" ]; then
        echo "      Criando .env a partir de .env.example..."
        cp .env.example .env
    else
        echo "      AVISO: .env.example não encontrado!"
    fi
else
    echo "      Arquivo .env já existe."
fi

echo "[2/3] Subindo os containers Docker..."
docker compose up -d --build

echo ""
echo "[3/3] Aguardando os serviços iniciarem..."
echo "      (O banco de dados, migrações do Django e os estáticos estão sendo preparados em background)"
echo ""
echo "=========================================="
echo "    Serviços em execução:                 "
echo "=========================================="
docker compose ps

echo ""
echo "URLs de Acesso:"
echo " - Portal Web:   http://localhost:8000/"
echo " - Adminer (BD): http://localhost:8080/"
echo ""
echo "Para importar os dados da Receita Federal, utilize:"
echo "  docker compose exec django python manage.py download_cnpj --only-latest"
echo "  docker compose exec django python manage.py load_cnpj --competencia YYYY-MM"
echo ""
echo "Tudo pronto!"
