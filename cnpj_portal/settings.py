"""
Configurações do portal de consulta CNPJ da Receita Federal.
"""
from pathlib import Path
from decouple import config

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = config("DJANGO_SECRET_KEY", default="dev-secret-key-change-in-production")
DEBUG = config("DJANGO_DEBUG", default=False, cast=bool)
ALLOWED_HOSTS = config("DJANGO_ALLOWED_HOSTS", default="localhost,127.0.0.1").split(",")

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django.contrib.postgres",
    "corsheaders",
    "cnpj",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "corsheaders.middleware.CorsMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "cnpj_portal.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "cnpj_portal.wsgi.application"

# Banco de dados
_db_url = config(
    "DATABASE_URL",
    default="postgres://cnpj:cnpj123@localhost:5432/cnpj",
)
# Parse manual de postgres://user:pass@host:port/dbname
def _parse_db_url(url: str) -> dict:
    from urllib.parse import urlparse
    r = urlparse(url)
    return {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": r.path.lstrip("/"),
        "USER": r.username,
        "PASSWORD": r.password,
        "HOST": r.hostname,
        "PORT": r.port or 5432,
        "OPTIONS": {"connect_timeout": 10},
    }

DATABASES = {"default": _parse_db_url(_db_url)}

CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
        "LOCATION": "cnpj-cache",
    }
}

# Internacionalização
LANGUAGE_CODE = "pt-br"
TIME_ZONE = "America/Sao_Paulo"
USE_I18N = True
USE_TZ = True

# Arquivos estáticos
STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "static"
STATICFILES_DIRS = []

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# Paginação padrão
CNPJ_PAGE_SIZE = 25

# Diretório de dados brutos
CNPJ_DATA_DIR = BASE_DIR / "data" / "raw"
CNPJ_LOGS_DIR = BASE_DIR / "logs"

# CORS — permite o frontend Vite (dev) e produção
CORS_ALLOWED_ORIGINS = config(
    "CORS_ALLOWED_ORIGINS",
    default="http://localhost:5173,http://127.0.0.1:5173",
).split(",")
CORS_ALLOW_ALL_ORIGINS = config("CORS_ALLOW_ALL_ORIGINS", default=False, cast=bool)


