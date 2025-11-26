
CREATE TABLE IF NOT EXISTS dim_data (
    id_data INTEGER PRIMARY KEY,
    date_time_iso TIMESTAMP,
    ano INTEGER,
    mes INTEGER,
    dia INTEGER,
    semestre INTEGER,
    trimestre INTEGER,
    dia_semana INTEGER,
    dia_ano INTEGER,
    is_weekend BOOLEAN,
    semana_ano INTEGER,
    estacao INTEGER
) WITH columnar;

CREATE TABLE IF NOT EXISTS dim_horario_clima (
    id_horario INTEGER PRIMARY KEY,
    hora INTEGER
) WITH columnar;

CREATE TABLE IF NOT EXISTS dim_horario_queimada (
    id_horario INTEGER PRIMARY KEY,
    id_horario_clima INTEGER REFERENCES dim_horario_clima(id_horario),
    hora INTEGER,
    minuto INTEGER
) WITH columnar;

CREATE TABLE IF NOT EXISTS dim_local_clima (
    id_local INTEGER PRIMARY KEY,
    regiao_uf TEXT,
    sigla_uf TEXT,
    nome_uf TEXT,
    nome_municipio TEXT,
    id_municipio INTEGER
) WITH columnar;

CREATE TABLE IF NOT EXISTS dim_local_queimada (
    id_local INTEGER PRIMARY KEY,
    id_local_clima INTEGER REFERENCES dim_local_clima(id_local),
    regiao_uf TEXT,
    sigla_uf TEXT,
    nome_uf TEXT,
    nome_municipio TEXT,
    id_municipio INTEGER,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    bioma TEXT
) WITH columnar;

CREATE TABLE IF NOT EXISTS fct_clima (
    id_data INTEGER REFERENCES dim_data(id_data),
    id_local INTEGER REFERENCES dim_local_clima(id_local),
    id_horario INTEGER REFERENCES dim_horario_clima(id_horario),
    temperatura DOUBLE PRECISION,
    umidade_relativa DOUBLE PRECISION,
    vento_velocidade DOUBLE PRECISION,
    vento_direcao DOUBLE PRECISION,
    co_ppb DOUBLE PRECISION,
    no2_ppb DOUBLE PRECISION,
    o3_ppb DOUBLE PRECISION,
    pm25_ugm3 DOUBLE PRECISION,
    so2_ugm3 DOUBLE PRECISION,
    precipitacao_dia DOUBLE PRECISION,
    PRIMARY KEY (id_data, id_local, id_horario)
) WITH columnar;

CREATE TABLE IF NOT EXISTS fct_queimada (
    id_data INTEGER REFERENCES dim_data(id_data),
    id_local INTEGER REFERENCES dim_local_queimada(id_local),
    id_horario INTEGER REFERENCES dim_horario_queimada(id_horario),
    risco_fogo DOUBLE PRECISION,
    potencia_radiativa_fogo DOUBLE PRECISION,
    dias_sem_chuva INTEGER,
    precipitacao DOUBLE PRECISION,
    PRIMARY KEY (id_data, id_local, id_horario)
) WITH columnar;
