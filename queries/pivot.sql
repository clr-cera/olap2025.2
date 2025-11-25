CREATE MATERIALIZED VIEW pivot_query_potencia_mes_uf_mat AS
SELECT
    dl.nome_uf,
    dd.mes,
    avg(fct_queimada.potencia_radiativa_fogo) as media_potencia
FROM fct_queimada
INNER JOIN public.dim_local_queimada dl ON fct_queimada.id_local = dl.id_local
INNER JOIN public.dim_data dd ON fct_queimada.id_data = dd.id_data
WHERE fct_queimada.potencia_radiativa_fogo is not null
GROUP BY (dl.nome_uf, dd.mes);
