from unittest.mock import patch

from django.urls import reverse

from cnpj.views import _format_cnpj


class TestCnpjApiEndpoints:
    @patch("cnpj.views.Estabelecimento.objects.values_list")
    @patch("cnpj.views.Estabelecimento.objects.filter")
    @patch("cnpj.views.CargaLog.objects.filter")
    def test_api_stats_returns_200(self, mock_carga, mock_filter, mock_estab_values, client):
        """Garante que o dashboard estático responda corretamente com mocks de banco"""
        # A view faz: competencias = sorted(Estabelecimento.objects.values_list(...).distinct(), reverse=True)
        # ultima = competencias[0] se tiver competencias
        mock_estab_values.return_value.distinct.return_value = ["2026-02", "2026-01"]
        mock_filter.return_value.count.return_value = 5000000
        mock_carga.return_value.count.return_value = 5

        url = reverse("cnpj:api_stats")
        response = client.get(url)
        assert response.status_code == 200

        data = response.json()
        assert data["total_empresas"] == 5000000
        assert data["total_competencias"] == 2
        assert "cargas_concluidas" in data

    @patch("cnpj.views.Estabelecimento.objects.values_list")
    def test_api_competencias_returns_200(self, mock_estab, client):
        """Garante que a lista de competências disponíveis responda via db mockado"""
        mock_estab.return_value.distinct.return_value = ["2026-02", "2026-01"]

        url = reverse("cnpj:api_competencias")
        response = client.get(url)
        assert response.status_code == 200
        assert "competencias" in response.json()

    def test_format_cnpj_utility(self):
        """Testa o utilitário interno de formatação do layout XX.XXX.XXX/XXXX-XX"""
        basico = "12345678"
        ordem = "0001"
        dv = "99"

        formatado = _format_cnpj(basico, ordem, dv)
        assert formatado == "12.345.678/0001-99"

    def test_format_cnpj_with_missing_zeros(self):
        """Testa utilitário caso os dados do banco venham truncados sem zeros à esquerda"""
        formatado = _format_cnpj("123", "1", "9")
        # 3 dígitos básicos devem virar 00000123 -> "00.000.123/0001-09"
        assert formatado == "00.000.123/0001-09"
