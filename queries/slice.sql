CREATE MATERIALIZED VIEW slice_precipitacao_estado_mes_setembro_mat AS
SELECT
    dd.mes,
    dlc.nome_uf,
    avg(fct_clima.precipitacao_dia) as media_precipitacao
FROM fct_clima
INNER JOIN public.dim_data dd on dd.id_data = fct_clima.id_data
INNER JOIN public.dim_local_clima dlc on dlc.id_local = fct_clima.id_local
WHERE dd.mes = 9
GROUP BY (dd.mes, dlc.nome_uf)
ORDER BY media_precipitacao DESC;
