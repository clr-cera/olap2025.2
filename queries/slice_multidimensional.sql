DROP MATERIALIZED VIEW slice_seca_norte_clima_mat;
CREATE MATERIALIZED VIEW slice_seca_norte_clima_mat AS
SELECT
    dd.dia_ano,
    dlc.nome_municipio,
    avg(fct_clima.co_ppb) as media_co_ppb,
    avg(fct_clima.pm25_ugm3) as media_pm25_ugm3,
    avg(fct_clima.o3_ppb) as media_o3_ppb
FROM fct_clima
    JOIN public.dim_data dd on dd.id_data = fct_clima.id_data
    JOIN public.dim_local_clima dlc on fct_clima.id_local = dlc.id_local
WHERE dd.mes >6 AND dd.mes < 10 AND regiao_uf = 'Norte'
GROUP BY dd.dia_ano, dlc.nome_municipio;
