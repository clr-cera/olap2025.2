SELECT alter_table_set_access_method('dim_data', 'heap');
SELECT alter_table_set_access_method('dim_horario_clima', 'heap');
SELECT alter_table_set_access_method('dim_horario_queimada', 'heap');
SELECT alter_table_set_access_method('dim_local_clima', 'heap');
SELECT alter_table_set_access_method('dim_local_queimada', 'heap');

VACUUM FULL dim_data;
VACUUM FULL dim_horario_clima;
VACUUM FULL dim_horario_queimada;
VACUUM FULL dim_local_clima;
VACUUM FULL dim_local_queimada;

SELECT alter_table_set_access_method('fct_clima', 'columnar');
SELECT alter_table_set_access_method('fct_queimada', 'columnar');

ALTER TABLE fct_clima SET
    (columnar.compression = none, columnar.stripe_row_limit = 1000000);
ALTER TABLE fct_queimada SET
    (columnar.compression = none, columnar.stripe_row_limit = 1000000);

VACUUM FULL fct_clima;
VACUUM FULL fct_queimada;

-- Indexes for dim_data
CREATE INDEX IF NOT EXISTS idx_dim_data_date ON dim_data(date_time_iso);
CREATE INDEX IF NOT EXISTS idx_dim_data_ano_mes ON dim_data(ano, mes);
CREATE INDEX IF NOT EXISTS idx_dim_data_trimestre ON dim_data(trimestre);

-- Indexes for dim_local_clima
CREATE INDEX IF NOT EXISTS idx_dim_local_clima_municipio ON dim_local_clima(id_municipio);
CREATE INDEX IF NOT EXISTS idx_dim_local_clima_uf ON dim_local_clima(sigla_uf);
CREATE INDEX IF NOT EXISTS idx_dim_local_clima_regiao ON dim_local_clima(regiao_uf);

-- Indexes for dim_local_queimada
CREATE INDEX IF NOT EXISTS idx_dim_local_queimada_municipio ON dim_local_queimada(id_municipio);
CREATE INDEX IF NOT EXISTS idx_dim_local_queimada_uf ON dim_local_queimada(sigla_uf);
CREATE INDEX IF NOT EXISTS idx_dim_local_queimada_local_clima ON dim_local_queimada(id_local_clima);
CREATE INDEX IF NOT EXISTS idx_dim_local_queimada_regiao ON dim_local_queimada(regiao_uf);
CREATE INDEX IF NOT EXISTS idx_dim_local_queimada_bioma ON dim_local_queimada(bioma);

-- Indexes for dim_horario_clima
CREATE INDEX IF NOT EXISTS idx_dim_horario_clima_hora ON dim_horario_clima(hora);

-- Indexes for dim_horario_queimada
CREATE INDEX IF NOT EXISTS idx_dim_horario_queimada_hora ON dim_horario_queimada(hora);
CREATE INDEX IF NOT EXISTS idx_dim_horario_queimada_horario_clima ON dim_horario_queimada(id_horario_clima);

-- Indexes for fct_queimada
CREATE INDEX IF NOT EXISTS idx_fct_queimada_data ON fct_queimada(id_data);
CREATE INDEX IF NOT EXISTS idx_fct_queimada_local ON fct_queimada(id_local);
CREATE INDEX IF NOT EXISTS idx_fct_queimada_horario ON fct_queimada(id_horario);

-- Indexes for fct_clima
CREATE INDEX IF NOT EXISTS idx_fct_clima_data ON fct_clima(id_data);
CREATE INDEX IF NOT EXISTS idx_fct_clima_local ON fct_clima(id_local);
CREATE INDEX IF NOT EXISTS idx_fct_clima_horario ON fct_clima(id_horario);
