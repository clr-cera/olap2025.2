SELECT alter_table_set_access_method('fct_clima', 'columnar');
SELECT alter_table_set_access_method('fct_queimada', 'columnar');

SELECT alter_columnar_table_set(
    'fct_clima',
    compression => 'none',
    stripe_row_count => 1000000
);

SELECT alter_columnar_table_set(
    'fct_queimada',
    compression => 'none',
    stripe_row_count => 1000000
);

VACUUM FULL fct_clima;
VACUUM FULL fct_queimada;

CREATE INDEX IF NOT EXISTS idx_fct_queimada_data ON fct_queimada(id_data);
CREATE INDEX IF NOT EXISTS idx_fct_queimada_local ON fct_queimada(id_local);
CREATE INDEX IF NOT EXISTS idx_fct_queimada_horario ON fct_queimada(id_horario);
CREATE INDEX IF NOT EXISTS idx_fct_clima_data ON fct_clima(id_data);
CREATE INDEX IF NOT EXISTS idx_fct_clima_local ON fct_clima(id_local);
CREATE INDEX IF NOT EXISTS idx_fct_clima_horario ON fct_clima(id_horario);
CREATE INDEX IF NOT EXISTS idx_dim_local_clima_regiao ON dim_local_clima(regiao_uf);
CREATE INDEX IF NOT EXISTS idx_dim_local_queimada_regiao ON dim_local_queimada(regiao_uf);
CREATE INDEX IF NOT EXISTS idx_dim_local_queimada_bioma ON dim_local_queimada(bioma);
CREATE INDEX IF NOT EXISTS idx_dim_data_trimestre ON dim_data(trimestre);