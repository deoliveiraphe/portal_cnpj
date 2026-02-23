# Deploy e Containers (Docker)

Toda a infraestrutura pode ser implantada rodando exclusivamente o stack listado no arquivo `./docker-compose.yml`, desobrigando o desenvolvedor (ou gestor de Cloud/DevOps) de instalar pacotes do ecossistema de Data Science do Python no disco físico do servidor (`pip`, `Pandas`).

## Levantando a Plataforma Inicial

Certifique-se que você tenha o arquivo `.env` preenchido a partir do `.env.example` na mesma pasta do compose, ajustando chaves nativas do framework do python (`DJANGO_SECRET_KEY`) antes de ir para ambiente de testes reais.

1. **Construa a(s) imagem(ns) declarada(s):**
    ```bash
    docker compose build
    ```
2. **Inicie o ecossistema:** O setup da Flag `-d` deixará os contâineres rodando de forma desacoplada no TTY (daemon mode). Dependências como o ORM de Database migrations só dispararão scripts após os *Healtchecks* nativos de TCP do PostgreSQL reportar sinal verde, blindando o seu start.
    ```bash
    docker compose up -d
    # Opcional, monitore via tailing nos logs se preferir
    docker compose logs -f django
    ```

### Volumes de Armazenamento
Ao invés de consumir disco primário do DB atoa, mantemos mapeamentos locais flexíveis:
* `data/raw`: Mantém os origens dos arquivos extraídos (.zip). Ideal mapear isso num disco AWS S3/NAS se em nuvem no futuro.
* `/var/lib/postgresql/data`: Volume dedicado p/ rodar no storage block puro de altíssima performance (fundamental pro volume de dados deste portal).
* `/logs`: Salva logs textuais parciais durante a execução do script paralelo de carga ETL.

### Visualizando os Módulos do Ambiente

Assumindo instâncias expostas na rota base padrão `localhost`: 
* **`http://localhost:8000/api/...`**: Rotas da API e Endpoints.
* **`http://localhost:8080/`**: Ambiente de monitoramento Front-end (caso o `Vite/Node` via pasta `./frontend` esteja rodando em dev-mode de porta via script npm).
* **`http://localhost:8081/`**: Ambiente gerêncial web de SQL (Container Adminer do Postgres). Evite em produção s/ Firewalls pesados de controle.

### Comandos da rotina do Sysadmin

Como todos os utilitários estão embarcados no conteiner do projeto Django propriamente dito, suas chamadas cron job via crontab nativo do linux só devem atachar `exec` através do CLI do Docker na stack ativa `cnpj_django`:

```bash
# Apagar dados de Cargas mal consolidadas no final de semana, p/ exemplificar
docker compose exec django python manage.py truncate_cnpj

# Disparar atualizador manual de rotina temporal e substituir histórico desatualizado com flag '--replace'
docker compose exec django python manage.py load_cnpj --start 2026-05 --end 2026-05 --replace
```
