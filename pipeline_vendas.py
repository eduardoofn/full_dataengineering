"""
DAG: pipeline_vendas_digital_corporativo
Pipeline ETL de Vendas com Geolocalização — Airflow 3.x

Fluxo:
    PostgreSQL (fonte) → Redshift (DW) → Parquet/HDFS (Lake) → PostgreSQL (Data Mart)

Queries e colunas validadas contra o analise.ipynb que rodou com sucesso.

Estratégia de carga:
    - dim_tempo/produto/cliente/vendedor : FULL (TRUNCATE + INSERT)
    - dim_localidade                     : FULL (DROP + CREATE + INSERT, igual notebook)
    - fato_venda                         : INCREMENTAL por ds (DELETE dia + INSERT dia)
    - Data Lake                          : INCREMENTAL — partição do dia (overwrite)
    - Data Mart                          : FULL a partir do Redshift

Credenciais: Airflow Variables (Admin → Variables).
    SOURCE_DATABASE_URL, REDSHIFT_DATABASE_URL, DASHBOARD_DATABASE_URL
    HDFS_URL, HDFS_USER, HDFS_DEST_PATH, LOCAL_PARQUET_FOLDER (opcional)

Compatibilidade — 100% psycopg2, zero SQLAlchemy, zero pd.read_sql/to_sql:
    - Leitura  → read_sql_pg:     psycopg2 cursor.execute + DataFrame manual
    - Escrita  → append_to_table: psycopg2 + execute_values
    - DDL/DML  → execute_query:   psycopg2 puro
"""

import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk import Variable

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from urllib.parse import urlparse

log = logging.getLogger(__name__)


# ── Helpers de URL ────────────────────────────────────────────────────────────

def _normalize_url(url):
    """redshift+psycopg2:// → postgresql:// para psycopg2."""
    if url and url.startswith("redshift+psycopg2://"):
        url = url.replace("redshift+psycopg2://", "postgresql://")
    return url

def get_source_url(): return _normalize_url(Variable.get("SOURCE_DATABASE_URL"))
def get_dw_url():     return _normalize_url(Variable.get("REDSHIFT_DATABASE_URL"))
def get_dm_url():     return _normalize_url(Variable.get("DASHBOARD_DATABASE_URL"))


# ── Helper de conexão ─────────────────────────────────────────────────────────

def _conn(url):
    p = urlparse(url)
    return psycopg2.connect(
        dbname=p.path[1:],
        user=p.username,
        password=p.password,
        host=p.hostname,
        port=p.port,
    )


# ── Leitura e escrita 100% psycopg2 ──────────────────────────────────────────

def read_sql_pg(query, url):
    """
    Lê DataFrame via psycopg2 cursor + fetchall.
    Não usa pd.read_sql — independente de versão de pandas/SQLAlchemy.
    """
    c = _conn(url)
    cur = c.cursor()
    try:
        cur.execute(query)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    finally:
        cur.close()
        c.close()

def execute_query(url, query):
    """Executa DDL/DML via psycopg2."""
    c = _conn(url)
    cur = c.cursor()
    try:
        cur.execute(query)
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        cur.close()
        c.close()

def append_to_table(url, df, table, schema="public", chunksize=2000):
    """INSERT bulk via psycopg2 + execute_values. NaN → NULL."""
    if df.empty:
        return
    df   = df.where(pd.notnull(df), None)
    cols = ", ".join(f'"{c}"' for c in df.columns)
    rows = [tuple(x) for x in df.to_numpy()]
    sql  = f'INSERT INTO {schema}."{table}" ({cols}) VALUES %s'
    c    = _conn(url)
    cur  = c.cursor()
    try:
        for i in range(0, len(rows), chunksize):
            execute_values(cur, sql, rows[i:i + chunksize], page_size=chunksize)
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        cur.close()
        c.close()

def _truncate_insert(url, df, table, schema="public", chunksize=2000):
    execute_query(url, f'TRUNCATE TABLE {schema}."{table}"')
    append_to_table(url, df, table, schema, chunksize)

def add_sk(df, col_name="sk"):
    df = df.copy().reset_index(drop=True)
    df.insert(0, col_name, range(1, len(df) + 1))
    return df


# ══════════════════════════════════════════════════════════════════════════════
# TASK 1 — extrair_banco_fonte
# Queries validadas contra analise.ipynb
# ══════════════════════════════════════════════════════════════════════════════
def extrair_banco_fonte(**context):
    url_src = get_source_url()
    url_rs  = get_dw_url()

    # ── DIM TEMPO ─────────────────────────────────────────────────────────────
    datas     = pd.date_range("2015-01-01", "2026-12-31")
    dim_tempo = add_sk(pd.DataFrame({
        "data":          datas,
        "ano":           datas.year,
        "mes":           datas.month,
        "trimestre":     datas.quarter,
        "nome_mes":      datas.month_name(),
        "dia_semana":    datas.day_name(),
        "is_fim_semana": datas.weekday >= 5,
    }), "sk_tempo")
    _truncate_insert(url_rs, dim_tempo, "dim_tempo")
    log.info(f"dim_tempo: {len(dim_tempo)} linhas")

    # ── DIM PRODUTO — validado no notebook ────────────────────────────────────
    df_produto = read_sql_pg("""
        SELECT p.id AS id_produto, p.nome, cat.descricao AS categoria,
               p.valor_venda AS preco_tabela
        FROM vendas.produto p
        JOIN vendas.categoria cat ON cat.id = p.id_categoria
    """, url_src)
    _truncate_insert(url_rs, add_sk(df_produto, "sk_produto"), "dim_produto")
    log.info(f"dim_produto: {len(df_produto)} linhas")

    # ── DIM CLIENTE — colunas validadas no notebook ───────────────────────────
    # Colunas: sk_cliente, id_pessoa, nome, cpf_cnpj, tipo_cliente
    df_cliente = read_sql_pg("""
        SELECT DISTINCT pf.id AS id_pessoa, pf.nome,
               pf.cpf AS cpf_cnpj, 'Pessoa Fisica' AS tipo_cliente
        FROM vendas.nota_fiscal nf
        JOIN geral.pessoa_fisica pf ON pf.id = nf.id_cliente
        UNION ALL
        SELECT DISTINCT pj.id AS id_pessoa, pj.razao_social AS nome,
               pj.cnpj AS cpf_cnpj, 'Pessoa Juridica' AS tipo_cliente
        FROM vendas.nota_fiscal nf
        JOIN geral.pessoa_juridica pj ON pj.id = nf.id_cliente
    """, url_src)
    _truncate_insert(url_rs, add_sk(df_cliente, "sk_cliente"), "dim_cliente")
    log.info(f"dim_cliente: {len(df_cliente)} linhas")

    # ── DIM VENDEDOR — validado no notebook ───────────────────────────────────
    df_vendedor = read_sql_pg("""
        SELECT DISTINCT pf.id AS id_pessoa, pf.nome
        FROM vendas.nota_fiscal nf
        JOIN geral.pessoa_fisica pf ON pf.id = nf.id_vendedor
    """, url_src)
    _truncate_insert(url_rs, add_sk(df_vendedor, "sk_vendedor"), "dim_vendedor")
    log.info(f"dim_vendedor: {len(df_vendedor)} linhas")

    # ── DIM LOCALIDADE — DROP+CREATE validado no notebook ─────────────────────
    # Cadeia: geral.endereco → geral.bairro → geral.cidade → geral.estado
    # Colunas: sk_localidade, id_endereco, id_pessoa, rua, numero, complemento,
    #          cep, bairro, cidade, estado, sigla_estado
    df_loc_raw = read_sql_pg("""
        SELECT
            e.id          AS id_endereco,
            e.id_pessoa,
            e.rua,
            e.numero,
            e.complemento,
            e.cep,
            b.descricao   AS bairro,
            c.descricao   AS cidade,
            est.descricao AS estado,
            est.sigla     AS sigla_estado
        FROM geral.endereco e
        LEFT JOIN geral.bairro  b   ON b.id   = e.id_bairro
        LEFT JOIN geral.cidade  c   ON c.id   = b.id_cidade
        LEFT JOIN geral.estado  est ON est.id  = c.id_estado
    """, url_src)

    # 1 linha por id_pessoa — mantém primeiro endereço se houver múltiplos
    dim_localidade = add_sk(
        df_loc_raw.drop_duplicates(subset="id_pessoa").reset_index(drop=True),
        "sk_localidade"
    )
    cols_loc = ["sk_localidade", "id_endereco", "id_pessoa", "rua", "numero",
                "complemento", "cep", "bairro", "cidade", "estado", "sigla_estado"]
    dim_localidade = dim_localidade[cols_loc]

    # DROP + CREATE garante schema correto (mesmo padrão do notebook)
    execute_query(url_rs, "DROP TABLE IF EXISTS public.dim_localidade")
    execute_query(url_rs, """
        CREATE TABLE public.dim_localidade (
            sk_localidade INTEGER,
            id_endereco   INTEGER,
            id_pessoa     INTEGER,
            rua           VARCHAR(400),
            numero        VARCHAR(50),
            complemento   VARCHAR(200),
            cep           VARCHAR(20),
            bairro        VARCHAR(200),
            cidade        VARCHAR(200),
            estado        VARCHAR(200),
            sigla_estado  VARCHAR(5)
        )
        DISTKEY(sk_localidade)
        SORTKEY(sigla_estado)
    """)
    append_to_table(url_rs, dim_localidade, "dim_localidade")
    log.info(f"dim_localidade: {len(dim_localidade)} linhas")

    context["ti"].xcom_push(key="contagens_dim", value={
        "dim_tempo":      len(dim_tempo),
        "dim_produto":    len(df_produto),
        "dim_cliente":    len(df_cliente),
        "dim_vendedor":   len(df_vendedor),
        "dim_localidade": len(dim_localidade),
    })


# ══════════════════════════════════════════════════════════════════════════════
# TASK 2 — carregar_redshift
# Query validada contra analise.ipynb
# ══════════════════════════════════════════════════════════════════════════════
def carregar_redshift(**context):
    """
    Carga FULL da fato_venda — igual ao analise.ipynb que rodou com sucesso.
    DROP + CREATE + INSERT de todas as vendas históricas.
    Idempotente: cada execução recria a tabela do zero.
    """
    url_src = get_source_url()
    url_rs  = get_dw_url()

    log.info("Carregando fato_venda FULL (todas as datas)...")

    df_raw = read_sql_pg("""
        SELECT
            nf.data_venda::date  AS data_venda,
            nf.id_cliente,
            nf.id_vendedor,
            inf.id_produto,
            nf.id                AS id_nota,
            inf.quantidade,
            inf.valor_unitario,
            inf.valor_venda_real,
            p.valor_venda
        FROM vendas.nota_fiscal nf
        JOIN vendas.item_nota_fiscal inf ON inf.id_nota_fiscal = nf.id
        JOIN vendas.produto p            ON p.id = inf.id_produto
    """, url_src)

    log.info(f"fato_raw: {len(df_raw):,} linhas lidas da fonte")

    if df_raw.empty:
        log.warning("Nenhuma venda encontrada na fonte.")
        context["ti"].xcom_push(key="fato_count", value=0)
        return

    df_fato = df_raw.copy()
    df_fato["data_venda"] = pd.to_datetime(df_fato["data_venda"]).dt.date

    dim_tempo      = read_sql_pg("SELECT sk_tempo, data FROM public.dim_tempo",                url_rs)
    dim_produto    = read_sql_pg("SELECT sk_produto, id_produto FROM public.dim_produto",      url_rs)
    dim_cliente    = read_sql_pg("SELECT sk_cliente, id_pessoa FROM public.dim_cliente",       url_rs)
    dim_vendedor   = read_sql_pg("SELECT sk_vendedor, id_pessoa FROM public.dim_vendedor",     url_rs)
    dim_localidade = read_sql_pg("SELECT sk_localidade, id_pessoa FROM public.dim_localidade", url_rs)

    dim_tempo["data"] = pd.to_datetime(dim_tempo["data"]).dt.date

    df_fato = (
        df_fato
        .merge(dim_tempo.rename(columns={"data": "data_venda"}),           on="data_venda",  how="left")
        .merge(dim_produto,                                                 on="id_produto",  how="left")
        .merge(dim_cliente.rename(columns={"id_pessoa": "id_cliente"}),    on="id_cliente",  how="left")
        .merge(dim_vendedor.rename(columns={"id_pessoa": "id_vendedor"}),  on="id_vendedor", how="left")
        .merge(dim_localidade.rename(columns={"id_pessoa": "id_cliente"}), on="id_cliente",  how="left")
    )

    df_fato["desconto"]     = (df_fato["valor_venda"] - df_fato["valor_unitario"]).round(2)
    df_fato["pct_desconto"] = (df_fato["desconto"] / df_fato["valor_unitario"] * 100).round(2)

    cols = ["sk_tempo", "sk_produto", "sk_cliente", "sk_vendedor", "sk_localidade",
            "id_nota", "quantidade", "valor_unitario", "valor_venda_real",
            "desconto", "pct_desconto"]
    df_fato = df_fato[cols]

    nulos = df_fato[["sk_cliente", "sk_localidade"]].isnull().sum()
    log.info(f"Nulos: sk_cliente={nulos['sk_cliente']:,} | sk_localidade={nulos['sk_localidade']:,}")

    # DROP + CREATE + INSERT igual ao notebook
    execute_query(url_rs, "DROP TABLE IF EXISTS public.fato_venda")
    execute_query(url_rs, """
        CREATE TABLE public.fato_venda (
            sk_tempo         INTEGER,
            sk_produto       INTEGER,
            sk_cliente       INTEGER,
            sk_vendedor      INTEGER,
            sk_localidade    INTEGER,
            id_nota          INTEGER,
            quantidade       INTEGER,
            valor_unitario   NUMERIC(18,2),
            valor_venda_real NUMERIC(18,2),
            desconto         NUMERIC(18,2),
            pct_desconto     NUMERIC(5,2)
        )
        DISTKEY(sk_tempo)
        SORTKEY(sk_tempo, sk_produto)
    """)
    append_to_table(url_rs, df_fato, "fato_venda", schema="public", chunksize=5000)

    log.info(f"fato_venda FULL: {len(df_fato):,} linhas carregadas")
    context["ti"].xcom_push(key="fato_count", value=len(df_fato))


# ══════════════════════════════════════════════════════════════════════════════
# TASK 3 — gravar_data_lake
# ══════════════════════════════════════════════════════════════════════════════
def gravar_data_lake(**context):
    import pyarrow as pa
    import pyarrow.parquet as pq

    execution_date = context["ds"]
    exec_dt        = datetime.strptime(execution_date, "%Y-%m-%d")
    ano, mes       = exec_dt.year, exec_dt.month
    url_rs         = get_dw_url()

    df = read_sql_pg(f"""
        SELECT dt.ano, dt.mes, dt.data,
               fv.quantidade, fv.valor_unitario, fv.valor_venda_real,
               dl.sigla_estado, dl.cidade
        FROM public.fato_venda fv
        JOIN public.dim_tempo dt ON dt.sk_tempo = fv.sk_tempo
        LEFT JOIN public.dim_localidade dl ON dl.sk_localidade = fv.sk_localidade
        WHERE dt.ano = {ano} AND dt.mes = {mes}
    """, url_rs)

    if df.empty:
        log.info("Nenhum dado para gravar no lake.")
        context["ti"].xcom_push(key="parquet_count", value=0)
        return

    local_folder   = Variable.get("LOCAL_PARQUET_FOLDER", default="lake/fato_venda")
    base           = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", local_folder)
    particao_local = os.path.join(base, f"ano={ano}", f"mes={mes}")
    os.makedirs(particao_local, exist_ok=True)

    arquivo     = os.path.join(particao_local, f"data_{execution_date}.parquet")
    df_sem_part = df.drop(columns=["ano", "mes"])
    pq.write_table(pa.Table.from_pandas(df_sem_part, preserve_index=False),
                   arquivo, compression="snappy")
    log.info(f"Parquet local: {len(df)} linhas → {arquivo}")

    hdfs_url       = Variable.get("HDFS_URL",       default="http://hadoop:9870")
    hdfs_user      = Variable.get("HDFS_USER",      default="root")
    hdfs_dest_path = Variable.get("HDFS_DEST_PATH", default="/vendas/fato_vendas/")

    try:
        from hdfs import InsecureClient
        client    = InsecureClient(hdfs_url, user=hdfs_user)
        dest_hdfs = f"{hdfs_dest_path.rstrip('/')}/ano={ano}/mes={mes}"
        if not client.status(dest_hdfs, strict=False):
            client.makedirs(dest_hdfs)
        with open(arquivo, "rb") as fdata:
            client.write(f"{dest_hdfs}/data_{execution_date}.parquet", fdata, overwrite=True)
        log.info(f"Upload HDFS: {dest_hdfs}")
    except Exception as e:
        log.warning(f"HDFS indisponível — mantendo lake local: {e}")

    context["ti"].xcom_push(key="parquet_count", value=len(df))


# ══════════════════════════════════════════════════════════════════════════════
# TASK 4 — popular_data_mart
# Lê direto do Redshift (mais robusto que depender do HDFS no daily)
# Agrega vendas_ano_mes e vendas_localidade — validado no notebook
# ══════════════════════════════════════════════════════════════════════════════
def popular_data_mart(**context):
    url_rs = get_dw_url()
    url_dm = get_dm_url()
    # data_atualizacao é DATE no banco real — usar date() não Timestamp
    hoje = pd.Timestamp.now().date()

    # ── Valida que fato_venda tem dados antes de agregar ──────────────────────
    n_fato = read_sql_pg("SELECT COUNT(*) AS n FROM public.fato_venda", url_rs).iloc[0, 0]
    log.info(f"fato_venda total no Redshift: {n_fato:,} linhas")
    if int(n_fato) == 0:
        raise ValueError(
            "fato_venda está vazia no Redshift — verifique se a task 2 (carregar_redshift) "
            "inseriu dados. A DAG é incremental por dia: certifique-se que o banco fonte "
            "tem vendas na data de execução."
        )

    # ── vendas_ano_mes_eduardo ────────────────────────────────────────────────
    df_fato = read_sql_pg("""
        SELECT dt.ano, dt.mes,
               fv.quantidade, fv.valor_unitario, fv.valor_venda_real
        FROM public.fato_venda fv
        JOIN public.dim_tempo dt ON dt.sk_tempo = fv.sk_tempo
    """, url_rs)
    log.info(f"Linhas lidas do Redshift para agregação: {len(df_fato):,}")

    vam = (
        df_fato.groupby(["ano", "mes"], as_index=False)
        .agg(qtde_vendida=("quantidade", "sum"),
             valor_total_real=("valor_venda_real", "sum"),
             valor_total_esperado=("valor_unitario", "sum"))
    )
    vam["qtde_vendida"]         = vam["qtde_vendida"].astype(int)
    vam["valor_total_real"]     = vam["valor_total_real"].round(2)
    vam["valor_total_esperado"] = vam["valor_total_esperado"].round(2)
    vam["data_atualizacao"]     = hoje   # DATE — igual ao DDL real da tabela

    # Usa o DDL real da tabela (data_atualizacao DATE, não TIMESTAMP)
    execute_query(url_dm, """
        CREATE TABLE IF NOT EXISTS public.vendas_ano_mes_eduardo (
            id                   BIGSERIAL PRIMARY KEY,
            ano                  INTEGER,
            mes                  INTEGER,
            qtde_vendida         INTEGER,
            valor_total_real     NUMERIC(18,2),
            valor_total_esperado NUMERIC(18,2),
            data_atualizacao     DATE
        )
    """)
    _truncate_insert(url_dm, vam, "vendas_ano_mes_eduardo", chunksize=1000)
    log.info(f"vendas_ano_mes_eduardo: {len(vam)} linhas | {hoje}")

    # ── vendas_localidade_eduardo ─────────────────────────────────────────────
    df_loc = read_sql_pg("""
        SELECT dt.ano,
               dl.sigla_estado, dl.estado, dl.cidade,
               fv.quantidade, fv.valor_unitario, fv.valor_venda_real
        FROM public.fato_venda fv
        JOIN public.dim_tempo dt           ON dt.sk_tempo = fv.sk_tempo
        LEFT JOIN public.dim_localidade dl ON dl.sk_localidade = fv.sk_localidade
        WHERE dl.sk_localidade IS NOT NULL
    """, url_rs)
    log.info(f"Linhas com localidade para agregação: {len(df_loc):,}")

    vloc = (
        df_loc.groupby(["ano", "sigla_estado", "estado", "cidade"], as_index=False)
        .agg(qtde_vendida=("quantidade", "sum"),
             valor_total_real=("valor_venda_real", "sum"),
             valor_total_esperado=("valor_unitario", "sum"))
    )
    vloc["qtde_vendida"]         = vloc["qtde_vendida"].astype(int)
    vloc["valor_total_real"]     = vloc["valor_total_real"].round(2)
    vloc["valor_total_esperado"] = vloc["valor_total_esperado"].round(2)
    vloc["pct_atingimento"]      = (
        vloc["valor_total_real"] / vloc["valor_total_esperado"] * 100
    ).round(1)

    execute_query(url_dm, """
        CREATE TABLE IF NOT EXISTS public.vendas_localidade_eduardo (
            id                   BIGSERIAL PRIMARY KEY,
            ano                  SMALLINT,
            sigla_estado         VARCHAR(5),
            estado               VARCHAR(200),
            cidade               VARCHAR(200),
            qtde_vendida         INTEGER,
            valor_total_real     NUMERIC(18,2),
            valor_total_esperado NUMERIC(18,2),
            pct_atingimento      NUMERIC(6,1)
        )
    """)
    _truncate_insert(url_dm, vloc, "vendas_localidade_eduardo", chunksize=1000)

    # Garante que a coluna existe (tabela pode ter sido criada antes sem ela)
    execute_query(url_dm, """
        ALTER TABLE public.vendas_localidade_eduardo
        ADD COLUMN IF NOT EXISTS data_atualizacao DATE
    """)
    execute_query(url_dm, f"""
        UPDATE public.vendas_localidade_eduardo
        SET data_atualizacao = '{hoje}'
    """)
    log.info(f"vendas_localidade_eduardo: {len(vloc)} linhas | data_atualizacao={hoje}")

    context["ti"].xcom_push(key="mart_counts", value={
        "vendas_ano_mes":    len(vam),
        "vendas_localidade": len(vloc),
    })


# ══════════════════════════════════════════════════════════════════════════════
# TASK 5 — validar_pipeline
# ══════════════════════════════════════════════════════════════════════════════
def validar_pipeline(**context):
    ti            = context["ti"]
    contagens_dim = ti.xcom_pull(task_ids="extrair_banco_fonte", key="contagens_dim") or {}
    fato_count    = ti.xcom_pull(task_ids="carregar_redshift",   key="fato_count")    or 0
    parquet_count = ti.xcom_pull(task_ids="gravar_data_lake",    key="parquet_count") or 0
    mart_counts   = ti.xcom_pull(task_ids="popular_data_mart",   key="mart_counts")   or {}

    log.info("=" * 60)
    log.info("RELATÓRIO DE VALIDAÇÃO DO PIPELINE")
    log.info(f"  Execução : {context['ds']}")
    log.info("-" * 60)
    log.info("  DIMENSÕES (Redshift):")
    for dim, cnt in contagens_dim.items():
        log.info(f"    {dim:<22}: {cnt:,} linhas")
    log.info(f"  fato_venda [{context['ds']}] : {fato_count:,} linhas")
    log.info(f"  Data Lake  [{context['ds']}] : {parquet_count:,} linhas")
    log.info("  DATA MART:")
    for mart, cnt in mart_counts.items():
        log.info(f"    {mart:<22}: {cnt:,} linhas")
    log.info("=" * 60)

    assert contagens_dim.get("dim_cliente",    0) > 0, "dim_cliente está vazia!"
    assert contagens_dim.get("dim_localidade", 0) > 0, "dim_localidade está vazia!"
    assert mart_counts.get("vendas_ano_mes",   0) > 0, "vendas_ano_mes_eduardo está vazia!"
    # vendas_localidade pode ser 0 se todos os clientes não têm endereço — apenas avisa
    if mart_counts.get("vendas_localidade", 0) == 0:
        log.warning("vendas_localidade_eduardo está vazia — clientes sem endereço cadastrado?")

    log.info("PIPELINE VALIDADO COM SUCESSO ✓")


# ══════════════════════════════════════════════════════════════════════════════
# DAG
# ══════════════════════════════════════════════════════════════════════════════
default_args = {
    "owner":            "airflow",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

with DAG(
    dag_id="pipeline_vendas_digital_corporativo",
    default_args=default_args,
    description=(
        "Pipeline ETL incremental de Vendas com Geolocalização | "
        "PostgreSQL → Redshift → Parquet/HDFS → PostgreSQL Data Mart"
    ),
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["vendas", "etl", "incremental", "geolocalizacao"],
) as dag:

    t1 = PythonOperator(task_id="extrair_banco_fonte", python_callable=extrair_banco_fonte)
    t2 = PythonOperator(task_id="carregar_redshift",   python_callable=carregar_redshift)
    t3 = PythonOperator(task_id="gravar_data_lake",    python_callable=gravar_data_lake)
    t4 = PythonOperator(task_id="popular_data_mart",   python_callable=popular_data_mart)
    t5 = PythonOperator(task_id="validar_pipeline",    python_callable=validar_pipeline)

    t1 >> t2 >> t3 >> t4 >> t5