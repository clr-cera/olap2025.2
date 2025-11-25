CREATE OR REPLACE VIEW cube_bioma_risco_fogo AS
SELECT
    dd.ano,
    dlq.bioma,
    dlq.nome_uf,
    avg(fct_queimada.risco_fogo) as media_risco_fogo
FROM fct_queimada
    INNER JOIN dim_data dd on fct_queimada.id_data = dd.id_data
    INNER JOIN dim_local_queimada dlq on fct_queimada.id_local = dlq.id_local
WHERE fct_queimada.risco_fogo is not null
GROUP BY dd.ano, CUBE (dlq.nome_uf, dlq.bioma)
