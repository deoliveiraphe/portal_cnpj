# Bem-vindo ao Portal de Dados Abertos CNPJ

O **Portal CNPJ** é uma solução completa para consumo, armazenamento otimizado e disponibilização dos dados públicos de Cadastro Nacional de Pessoa Jurídica (CNPJ) fornecidos pela Secretaria Especial da Receita Federal do Brasil (RFB).

## Qual o problema que este projeto resolve?

Lidar com a base de dados abertos da Receita Federal é historicamente difícil devido a:
1. **Volume de dados**: A base possui dezenas de milhões de empresas, estabelecimentos e sócios, totalizando dezenas de gigabytes de dados descompactados.
2. **Formato engessado**: Os dados vêm no formato de arquivos ZIP divididos, contendo arquivos `.CSV` com um layout fixo e por vezes colunas não estruturadas de forma amigável para bancos relacionais diretos.
3. **Alto consumo de I/O**: Descompactar, tratar e inserir milhões de linhas via instruções `INSERT` tradicionais levaria dias em um banco SQL padrão.

## A Solução (Mihos)

Construímos um pipeline unificado em **Python, Django, Pandas e PostgreSQL** com os seguintes objetivos:
- Dispensar extração física em disco (os `.zip` são analisados na memória).
- Maximizar velocidade de inserção no banco de dados com a instrução `COPY` nativa do PostgreSQL.
- Servir uma plataforma amigável de consultas de mercado e visualização da evolução do quadro societário das empresas de acordo com as "competências" temporais disponibilizadas pelo governo.
- Prover uma **API Rest** que possa ser abstraída e consumida por robôs / sistemas terceiros.

## Como navegar por esta documentação:

A documentação está dividida nas seguintes verticais:
1. [Arquitetura e Componentes Técnica](arquitetura.md)
2. [O Processo ETL (Download e Carga)](etl_pipeline.md)
3. [Modelagem do Banco de Dados](banco_de_dados.md)
4. [Painel e Endpoints da API REST](painel_api.md)
5. [Guia de Deploy (Docker Compose)](deploy.md)
