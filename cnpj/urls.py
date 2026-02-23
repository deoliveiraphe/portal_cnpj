from django.urls import path
from . import views

app_name = "cnpj"

urlpatterns = [
    path("api/stats/", views.api_stats, name="api_stats"),
    path("api/competencias/", views.api_competencias, name="api_competencias"),
    path("api/busca/", views.api_busca, name="api_busca"),
    path("api/cnpj/<str:cnpj_basico>/", views.api_cnpj_detalhe, name="api_cnpj_detalhe"),
]
