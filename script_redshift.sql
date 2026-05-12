-- Dimensão tempo (gerada via Python ou dbt)
CREATE TABLE dim_tempo (
    sk_tempo    INTEGER PRIMARY KEY,  -- surrogate key
    data        DATE,
    ano         SMALLINT,
    mes         SMALLINT,
    trimestre   SMALLINT,
    nome_mes    VARCHAR(20),
    dia_semana  VARCHAR(20),
    is_fim_semana BOOLEAN
)
SORTKEY(data);  -- Redshift: otimiza filtros por data

-- Dimensão produto
CREATE TABLE dim_produto (
    sk_produto  INTEGER PRIMARY KEY,
    id_produto  INTEGER,
    nome        VARCHAR(200),
    categoria   VARCHAR(100),
    preco_tabela NUMERIC(18,2)
)
DISTKEY(sk_produto);

drop table dim_cliente 

-- Dimensão cliente
CREATE TABLE dim_cliente (
    sk_cliente   INTEGER PRIMARY KEY,
    id_pessoa    INTEGER,
    nome         VARCHAR(200),
    cpf_cnpj          VARCHAR(14),
    tipo_cliente VARCHAR(20)
)
DISTKEY(sk_cliente);

-- Dimensão vendedor
CREATE TABLE dim_vendedor (
    sk_vendedor INTEGER PRIMARY KEY,
    id_pessoa   INTEGER,
    nome        VARCHAR(200)
)
DISTKEY(sk_vendedor);

drop table fato_venda

-- Tabela fato (grão: 1 linha = 1 item de nota fiscal)
CREATE TABLE fato_venda (
    sk_tempo      INTEGER REFERENCES dim_tempo(sk_tempo),
    sk_produto    INTEGER REFERENCES dim_produto(sk_produto),
    sk_cliente    INTEGER REFERENCES dim_cliente(sk_cliente),
    sk_vendedor   INTEGER REFERENCES dim_vendedor(sk_vendedor),
    id_nota       INTEGER,
    quantidade    INTEGER,
    valor_unitario   NUMERIC(18,2),
    valor_venda_real NUMERIC(18,2),
    desconto         NUMERIC(18,2),
    pct_desconto     NUMERIC(5,2)
)
DISTKEY(sk_tempo)
SORTKEY(sk_tempo, sk_produto);