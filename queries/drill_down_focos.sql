SELECT
    dim_local_queimada.nome_uf,
    dim_local_queimada.nome_municipio,
    count(*) OVER (PARTITION BY nome_municipio) as count_focus_municipio,
    count(*) OVER (PARTITION BY nome_uf) as count_focus_uf
FROM fct_queimada
INNER JOIN dim_local_queimada ON fct_queimada.id_local = dim_local_queimada.id_local
GROUP BY (dim_local_queimada.nome_uf, dim_local_queimada.nome_municipio);
