
-- Schema for direct output of ETL pipeline
CREATE SCHEMA IF NOT EXISTS etl_result;

CREATE TABLE IF NOT EXISTS etl_result.dim_data
(
    id bigint,
    ano smallint NOT NULL,
    semestre smallint NOT NULL,
    trimestre smallint NOT NULL,
    mes smallint NOT NULL,
    dia smallint NOT NULL,
    dia_da_semana smallint NOT NULL,
    dia_do_ano smallint NOT NULL,
    numero_semana smallint NOT NULL,
    fim_de_semana boolean NOT NULL,
    estacao character varying(255) NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS etl_result.dim_local
(
    id BIGINT,
    municipio VARCHAR(255) NOT NULL,
    id_municipio BIGINT NOT NULL,
    estado VARCHAR(255) NOT NULL,
    regiao VARCHAR(255) NOT NULL,
    latitude FLOAT,
    longitude FLOAT,
    bioma VARCHAR(255),
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS etl_result.dim_horario
(
    id BIGINT,
    hora SMALLINT NOT NULL,
    minuto SMALLINT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS etl_result.fct_queimada
(
    data_fk BIGINT,
    local_fk BIGINT,
    horario_fk BIGINT,
    risco_fogo FLOAT,
    frp FLOAT,
    dias_sem_chuva SMALLINT,
    FOREIGN KEY (data_fk)
        REFERENCES  etl_result.dim_data(id),
    FOREIGN KEY (local_fk)
        REFERENCES etl_result.dim_local(id),
    FOREIGN KEY (horario_fk)
        REFERENCES etl_result.dim_horario(id),
    PRIMARY KEY (data_fk, local_fk, horario_fk)
);

CREATE TABLE IF NOT EXISTS etl_result.fct_clima
(
    data_fk         BIGINT      NOT NULL,  -- Chave da Dimensão Data
    local_fk        BIGINT      NOT NULL,  -- Chave da Dimensão Local (Município)
    horario_fk      BIGINT      NOT NULL,  -- Chave da Dimensão Horário

    temperatura         FLOAT,
    precipitacao_dia        FLOAT,
    umidade_relativa    FLOAT,
    vento_velocidade    FLOAT,
    vento_direcao       SMALLINT,
    co_ppb              FLOAT,
    no2_ppb             FLOAT,
    o3_ppb              FLOAT,
    pm25_ugm3           FLOAT,
    so2_ugm3            FLOAT,

    PRIMARY KEY (data_fk, local_fk, horario_fk),

    FOREIGN KEY (data_fk) REFERENCES etl_result.dim_data(id),
    FOREIGN KEY (local_fk) REFERENCES etl_result.dim_local(id),
    FOREIGN KEY (horario_fk) REFERENCES etl_result.dim_horario(id)
);

