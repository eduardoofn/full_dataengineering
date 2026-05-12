# 🚀 Pipeline de Engenharia de Dados — Vendas Corporativo

Pipeline ETL completo de ponta a ponta, orquestrado com Apache Airflow, cobrindo ingestão, modelagem dimensional, Data Lake, Data Mart e visualização em dashboard.

---

## 🏗️ Arquitetura

```
PostgreSQL (Fonte)
       │
       ▼
  [ Airflow DAG ]
       │
       ├── T1: Extrair banco fonte  ──► Redshift (dim_*)
       │
       ├── T2: Carregar Redshift    ──► Redshift (fato_venda)
       │
       ├── T3: Gravar Data Lake     ──► HDFS (Parquet particionado)
       │
       ├── T4: Popular Data Mart    ──► PostgreSQL (vendas_ano_mes / vendas_localidade)
       │
       └── T5: Validar pipeline     ──► Relatório de contagens + XCom
```

---

## 📐 Modelo Estrela (Redshift)

```
                    ┌─────────────┐
                    │  dim_tempo  │
                    │  sk_tempo   │
                    └──────┬──────┘
                           │
┌──────────────┐    ┌──────▼──────┐    ┌───────────────┐
│  dim_produto │◄───│  fato_venda │───►│  dim_cliente  │
│  sk_produto  │    │  sk_tempo   │    │  sk_cliente   │
└──────────────┘    │  sk_produto │    └───────────────┘
                    │  sk_cliente │
┌──────────────┐    │  sk_vendedor│    ┌───────────────┐
│ dim_vendedor │◄───│  sk_local.  │───►│dim_localidade │
│  sk_vendedor │    │  id_nota    │    │ sk_localidade │
└──────────────┘    │  quantidade │    └───────────────┘
                    │  vl_unitário│
                    │  vl_real    │
                    │  desconto   │
                    │  pct_desc.  │
                    └─────────────┘
```

---

## 🛠️ Stack Tecnológica

| Camada | Tecnologia |
|---|---|
| Orquestração | Apache Airflow 3.x |
| Banco Fonte | PostgreSQL (transacional) |
| Data Warehouse | Amazon Redshift |
| Data Lake | HDFS + Apache Parquet (Snappy) |
| Data Mart | PostgreSQL |
| Dashboard | Dash / Plotly |
| Containerização | Docker + Docker Compose |
| Linguagem | Python 3.12 |
| Bibliotecas | psycopg2, pandas, pyarrow, python-dotenv |

---

## 📁 Estrutura do Projeto

```
projeto/
├── analise.ipynb              # Notebook de exploração e prototipagem
├── app.py                     # Dashboard Dash/Plotly (6 gráficos + 5 KPIs)
├── script_redshift.sql        # DDL completo do modelo estrela no Redshift
├── requirements.txt           # Dependências Python do projeto
├── Dockerfile                 # Container do dashboard
├── docker-compose.yml         # Orquestração de todos os serviços
├── .env.example               # Template de variáveis sem credenciais
├── .gitignore
├── docs/
│   └── screenshots/           # Evidências do pipeline em execução
│       ├── 00_arquitetura_infografico.jpg
│       ├── 01_datamart_pgadmin.png
│       ├── 02_hadoop_hdfs.png
│       ├── 03_airflow_dag.png
│       ├── 04_docker_containers.png
│       ├── 05_redshift_query.png
│       └── 06_dash_dashboard.png
├── dags/
│   └── pipeline_vendas.py     # DAG principal com 5 tasks (psycopg2 puro)
└── lake/                      # Data Lake local (ignorado pelo Git)
    └── fato_venda/
        └── ano=YYYY/mes=MM/data_historico_atualizado.parquet
```

---

## ⚙️ Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto baseado no `.env.example`:

```env
# Banco de dados fonte (PostgreSQL transacional)
SOURCE_DATABASE_URL=postgresql://user:senha@host:5432/dbname

# Data Warehouse (Amazon Redshift)
REDSHIFT_DATABASE_URL=postgresql://user:senha@host:5439/vendas_dw
REDSHIFT_SSLMODE=require

# Data Mart (PostgreSQL painel)
DASHBOARD_DATABASE_URL=postgresql://user:senha@host:5432/dashboard

# HDFS
HDFS_URL=http://hadoop:9870
HDFS_USER=root
HDFS_DEST_PATH=/vendas/fato_vendas/

# Parquet local
LOCAL_PARQUET_FOLDER=lake/fato_venda

# Agregação
AGGREGATION_YEARS=2024,2025,2026

# Dashboard
DASH_PORT=8050
```

No Airflow, as variáveis são cadastradas em **Admin → Variables** com os mesmos nomes.

---

## 🔄 DAG — pipeline_vendas_digital_corporativo

```
T1 extrair_banco_fonte
   └── Carrega dim_tempo, dim_produto, dim_cliente,
       dim_vendedor e dim_localidade no Redshift (FULL)
       
T2 carregar_redshift
   └── Carrega fato_venda completa no Redshift (DROP + CREATE + INSERT)
       Resolução de todas as surrogate keys via merge em Python
       Desconto calculado em Python (valor_venda - valor_unitario)
       
T3 gravar_data_lake
   └── Exporta partição do mês do Redshift para Parquet (Snappy)
       Envia ao HDFS particionado por ano/mes
       Mantém cópia local como fallback
       
T4 popular_data_mart
   └── Agrega fato_venda → vendas_ano_mes_eduardo
       Cruza com dim_localidade → vendas_localidade_eduardo
       
T5 validar_pipeline
   └── Verifica contagens em todas as tabelas
       Loga métricas via XCom
       Falha com AssertionError se alguma tabela crítica estiver vazia
```

**Agendamento:** `@daily` | **Retries:** 1 | **Catchup:** False

---

## 🚀 Como Executar

### 1. Clone o repositório

```bash
git clone https://github.com/seu-usuario/pipeline-vendas-digital.git
cd pipeline-vendas-digital
```

### 2. Configure as variáveis de ambiente

```bash
cp .env.example .env
# edite o .env com suas credenciais
```

### 3. Suba os containers

```bash
docker compose up -d
```

### 4. Configure as Variables no Airflow

Acesse `http://localhost:8080` → Admin → Variables e cadastre todas as variáveis do `.env`.

### 5. Ative a DAG

Na UI do Airflow, ative a DAG `pipeline_vendas_digital_corporativo` e dispare manualmente o primeiro run.

### 6. Acesse o Dashboard

```bash
http://localhost:8050
```

---

## 🧠 Decisões Técnicas

**Por que psycopg2 puro em vez de `df.to_sql()`?**
O ambiente Airflow usa pandas 2.x, que removeu suporte a conexões DBAPI2 no `pd.read_sql`. Para garantir compatibilidade total independente de versão de pandas ou SQLAlchemy, toda leitura usa `cursor.execute + fetchall` e toda escrita usa `execute_values` do psycopg2, sem passar pelo pandas I/O.

**Por que carga FULL na fato_venda?**
Os dados históricos do banco fonte já existiam antes da implantação da DAG. A carga FULL via DROP + CREATE garante idempotência e schema correto a cada execução, alinhada com a prototipagem no notebook.

**Por que dim_localidade usa DROP + CREATE?**
A cadeia `endereco → bairro → cidade → estado` pode ter schema variável. O DROP + CREATE garante que a tabela no Redshift sempre reflete a estrutura mais recente sem resíduo de versões antigas.

---

## 📊 Resultados

| Tabela | Linhas |
|---|---|
| dim_tempo | 4.383 |
| dim_produto | 233 |
| dim_cliente | ~15.900 |
| dim_vendedor | 24 |
| dim_localidade | ~15.000 |
| fato_venda | 343.489 |
| vendas_ano_mes_eduardo | 137 |
| vendas_localidade_eduardo | — |

---

## 👨‍💻 Autor

**Eduardo** — Especialista em BI, SQL e Engenharia de Dados com 10 anos de experiência.
Apaixonado por transformar dados brutos em decisões estratégicas.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Eduardo-blue?logo=linkedin)](https://linkedin.com/in/eduardoofn)
[![GitHub](https://img.shields.io/badge/GitHub-seu--usuario-black?logo=github)](https://github.com/eduardoofn)
