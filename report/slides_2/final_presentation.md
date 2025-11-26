---
marp: true
theme: rose-pine-moon
style: |
  .columns {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 1rem;
  }
---

# **Cinzas do Brasil**

## Análise Multidimensional de Dados de Queimadas

Projeto final da Disciplina Processamento Analítico de Dados
Realizado por:
Felipe Carneiro Machado - 14569373
Lívia Lelis - 12543822
Clara Ernesto de Carvalho - 14559479

---

# Objetivo

- Consolidação de dados do INPE (Instituto Nacional de Pesquisas Espaciais) relativos a focos de queimadas e clima
- Criação de um Data Warehouse com dados históricos (desde 2003 (alguém corrige essa data))
- Geração de visualizações baseadas em consultas analíticas para tomada estratégica de decisões

---

# Visão geral da Arquitetura

![](diagrams/Arquitetura_OLAP.drawio.png)

---

# Organização do Data Warehouse

Constelação de fatos corrigida

<div class="columns">
<div>
<ul>
<li>Remoção da tabela bridge</li>
<li>Dimensão Data conformada</li>
<li>Dimensões horário e local do esquema Queimada (granularidade mais fina) possuem Chaves Estrangeiras para as mesmas dimensões no esquema Clima (granularidade mais grossa)</li>
</ul>
</div>
<center>
<img align="center" src="diagrams/full_schema.png" width="400">
</center>

---

# Organização do <br>Data Warehouse

## Queimadas

Esquema estrela corrigido

- Dessa vez, fizemos o esquema estrela sem pensar na posterior unificação

![bg right fit](./diagrams/queimadas_schema.png)

---

# Organização do <br>Data Warehouse

## Clima

Esquema estrela corrigido

- Dessa vez, fizemos o esquema estrela sem pensar na posterior unificação

## ![bg right fit](./diagrams/clima_schema.png)

---

# Infraestrutura

## Provisionamento com Terraform

Provisionamos com Terraform:

- Base de dados PostgreSQL
- Jobs Spark para ETL
- Superset para visualização dos dados

## ![bg right fit](https://www.datocms-assets.com/2885/1679095195-devdot-terraform_lm.png)

---

# Infraestrutura

## Google Cloud Platform

Tava de graça 🙏 (Deram 1800 reais de crédito pra gente)

## ![bg right fit](https://media.licdn.com/dms/image/v2/D5612AQEjZljStRxpOQ/article-cover_image-shrink_720_1280/article-cover_image-shrink_720_1280/0/1691568005689?e=2147483647&v=beta&t=PbaaQ-GXlNVWUG6ct-poejXcFb2-i6TuaqqrDfwwnZY)

---

# Extração dos Dados

Foram utilizadas 4 fontes:

- Dataset de focos de queimadas do INPE -> extraído diretamente do BigQuery

- Dataset de dados climáticos do SISAM -> extraído diretamente do BigQuery

- Relação de munícipios e seus códigos pelo IBGE -> CSV obtido de fontes públicas

- Relação de estados e as regiões às quais pertencem -> CSV obtido de fontes públicas

---

# Transformação

Pré-processamento dos dados:

- Tratamento de valores faltantes
- Computação de atributos derivados (ex: estação do ano)
- Atribuição de tipos de dados corretos

---

# Transformação

Criação de dimensões e tabelas de fatos:

- Dimensão Data gerada a partir de união e projeção das tabelas de Queimadas e Clima
- Dimensões Local geradas a partir da junção e projeção de cada tabela com as relações de Municípios e regiões
- Dimensão Horário preenchida proceduralmente com todos os valores de horas e minutos
- Chaves Estrangeiras das tabelas de fatos preenchidas através de junção com as tabelas de dimensões

---

# Carregamento

Dados carregados em um RSGBD (PostgreSQL)

Indíces criados para colunas de frequente acesso:

- Chaves estrangeiras nas tabelas de fatos e dimensões do esquema Queimadas
- Timestamp, Mes e ano para Data
- Hora para Horário
- UF e município para Local

---

# Consultas analíticas e visualização

- Consultas implementadas em SQL
- Visualizações construídas com Apache Superset

---

# Queimadas por Estado e Municipio

<center>
<img src="charts/queimadaByUfCity.png" height=500vh>
</center>

---

# Risco de Fogo por Bioma e Estado

<center>
<img src="charts/riscoFogoByBiomaUf.png" height=500vh>
</center>

---

# Risco de Fogo por Estado e Bioma

<center>
<img src="charts/riscoFogoByUfBioma.png" height=500vh>
</center>

---

# Focos de Incêndio relacionados à poluição e temperatura, por hora do dia

<center>
<img src="charts/focosByTempPolHora.png" height=450vh>
</center>

---

# Potência Radiativa média por Umidade média para os Estados, com Região indicada

<center>
<img src="charts/frpHumidity.png" height=450vh>
</center>

---

# Potência Radiativa média e Umidade média por Trimestre e Região

<center>
<img src="charts/frpHumidityTrimestreRegion.png" height=450vh>
</center>

---

# Potência Radiativa por Mês e Estado

<center>
<img src="charts/frpMesUF.png" height=450vh>
</center>

---

# Potência Radiativa por Estado com filtro para Mês

<center>
<img src="charts/frpUfMesFilter.png" height=450vh>
</center>

---

# Precipitação por Estado no Mês de Setembro

<center>
<img src="charts/rainByStateSeptember.png" height=450vh>
</center>

---

# Qualidade do ar na região Norte na época de secas

<center>
<img src="charts/airQualityNoth.png" height=450vh>
</center>

---

# Conclusões

- Correlações identificadas entre dados de queimadas e clima (Ex: FRP x Umidade)

- Identificação de biomas e estado em maior risco de focos de incêndios, além de estados e cidades com maior incidência

- Análise de comportamentos específicos para cada estado quanto a intensidade de queimadas

---

# Referências

- [Google Cloud Platform](https://bombatec.com.br/shop/manufacturer-site?&transition=top97566919052470)
- [Terraform](https://developer.hashicorp.com/terraform)

---

# Código Fonte

## [Github](https://github.com/clr-cera/olap2025.2)

## ![bg right fit](./qrcodes/github.png)

---

# **Perguntas?**

UwU
