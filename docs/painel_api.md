# Painel Web e API Rest

A plataforma não precisa de SSR (Server-Side Rendering) tradicional recarregando todas as listagens ou de pacotes front-end monolíticos. O Frontend Vue/React ou Mobile App (desacoplado) requisita dados em formato JSON pelas Views do Django, otimizando o tamanho das *Payloads*.

## Endpoints Públicos disponíveis

### `1. GET /api/stats/`
Estatísticas gerais da plataforma para uso na renderização de dashbards da página inicial.

* **Exemplo de Resposta (JSON):**
    ```json
    {
      "total_empresas": 60533201,
      "total_competencias": 3,
      "ultima_competencia": "2026-01",
      "cargas_concluidas": 21
    }
    ```

### `2. GET /api/competencias/`
Recupera uma lista cronológica contendo apenas meses (YYYY-MM) que sofreram cargas totais com logs indicando `SUCESSO` e podem ser servidas sem falhas aos usuários no filtro temporal.

### `3. GET /api/busca/`
Principal Endpoint de listagem rápida e cruza resultados da base relacional limitados pelo parâmetro da paginação. Dispensa tabelas Elasticsearch baseando-se em avaliações dinâmicas de Chaves de texto ou Subqueries no banco (EX: "Razão social" busca dentro de CNPJ Base em paralelo de forma transparente sem locks).

* **Query Params Suportados (via querystring)**:
    - `q` (Livre/CNPJ/NomeFantasia/RazãoSocial).
    - `competencia` (Define em qual base de tempo atuar, assume default p/ a *última_competencia*).
    - `uf`, `municipio`, `cnae`.
    - `situacao` (02=Ativa, 04=Inapta...), `porte` (01, 03, 05).
    - Binários (S ou N): `simples` e `mei`.
    - `page` (Padrão 1, limites maximos estabelecidos no backend).

### `4. GET /api/cnpj/<cnpj_basico>/`
Traz o consolidado completo de todas as planilhas agregadas sobre o negócio (Sócio, Ente de Responsabilidade, Endereçamento Físico e Status no Ministério Fazenda), gerando árvore familiar se for Matriz/Filial agrupadas no mesmo digíto base informando os últimos quatorze dígitos.

- Possibilita acessar `competencias_disponiveis` da empresa permitindo a tela renderizar em gráficos do tipo _Time-Series_ flutuações de status cadastral/situação do CPF da matriz baseados na competência acessada em `?competencia=YYYY-MM`.
