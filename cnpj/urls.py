from django.urls import path
from . import views
from . import views_html

app_name = "cnpj"

urlpatterns = [
    # Rotas Frontend (HTML)
    path("", views_html.home, name="home"),
    path("busca/", views_html.busca, name="busca"),
    path("cnpj/<str:cnpj_basico>/", views_html.detalhe, name="detalhe"),

    # Rotas API (JSON)
    path("api/stats/", views.api_stats, name="api_stats"),
    path("api/competencias/", views.api_competencias, name="api_competencias"),
    path("api/busca/", views.api_busca, name="api_busca"),
    path("api/cnpj/<str:cnpj_basico>/", views.api_cnpj_detalhe, name="api_cnpj_detalhe"),
]
