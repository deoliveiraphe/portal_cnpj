#!/bin/bash
set -e

# Extrai host do DATABASE_URL (formato: postgres://user:pass@host:port/db)
DB_HOST="${PGHOST:-postgres}"
DB_USER="${PGUSER:-cnpj}"
DB_NAME="${PGDATABASE:-cnpj}"

if [ -n "$DATABASE_URL" ]; then
  # Remove o prefixo postgres://user:pass@
  _tmp="${DATABASE_URL#*@}"
  # Extrai host:port/db
  DB_HOST="${_tmp%%:*}"
  DB_HOST="${DB_HOST%%/*}"
fi

echo "==> Aguardando PostgreSQL em ${DB_HOST}..."
until pg_isready -h "${DB_HOST}" -p 5432 -U "${DB_USER}" -d "${DB_NAME}" -q; do
  echo "    Postgres em ${DB_HOST}:5432 não disponível — aguardando..."
  sleep 2
done
echo "==> PostgreSQL disponível!"

echo "==> Aplicando migrações..."
python manage.py migrate --noinput

echo "==> Coletando arquivos estáticos..."
python manage.py collectstatic --noinput

echo "==> Iniciando Gunicorn..."
exec gunicorn cnpj_portal.wsgi:application \
  --bind 0.0.0.0:8000 \
  --workers 4 \
  --timeout 300 \
  --access-logfile - \
  --error-logfile -
