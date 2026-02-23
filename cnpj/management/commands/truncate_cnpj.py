"""
Management command para apagar todos os dados do CNPJ de forma instantânea (TRUNCATE).
"""
from django.core.management.base import BaseCommand
from django.db import connection

class Command(BaseCommand):
    help = "Apaga todas as tabelas CNPJ do banco (TRUNCATE CASCADE) de forma instantânea."

    def add_arguments(self, parser):
        parser.add_argument(
            "--yes",
            action="store_true",
            help="Confirma a exclusão sem perguntar",
        )

    def handle(self, *args, **options):
        if not options["yes"]:
            confirm = input("⚠️  ATENÇÃO: ISSO APAGARÁ **TODOS** OS DADOS DO CNPJ! Tem certeza? (s/N): ")
            if confirm.lower() != 's':
                self.stdout.write(self.style.WARNING("Operação cancelada."))
                return

        self.stdout.write(self.style.WARNING("Executando TRUNCATE CASCADE em todas as tabelas CNPJ..."))
        
        tabelas = [
            "cnpj_empresa",
            "cnpj_estabelecimento",
            "cnpj_socio",
            "cnpj_simples",
            "cnpj_cnae",
            "cnpj_municipio",
            "cnpj_pais",
            "cnpj_natureza",
            "cnpj_qualificacao",
            "cnpj_motivo",
            "cnpj_cargalog"
        ]

        with connection.cursor() as cur:
            for tabela in tabelas:
                try:
                    cur.execute(f"TRUNCATE TABLE {tabela} CASCADE")
                    self.stdout.write(self.style.SUCCESS(f"✔ Tabela {tabela} truncada."))
                except Exception as e:
                    pass

        self.stdout.write(self.style.SUCCESS("\n🚀 Banco de dados CNPJ limpo com sucesso!"))
