CREATE MATERIALIZED VIEW drill_across_umidade_potencia_mat AS
WITH joined_clima AS
    (SELECT
         dlc.regiao_uf,
         dd.trimestre,
         avg(fct_clima.umidade_relativa) media_umidade_relativa
     FROM fct_clima
     INNER JOIN public.dim_local_clima dlc on dlc.id_local = fct_clima.id_local
     INNER JOIN public.dim_data dd on fct_clima.id_data = dd.id_data
     GROUP BY dlc.regiao_uf, dd.trimestre
     ),
    joined_queimada AS
    (SELECT
         d.trimestre,
         dlq.regiao_uf,
         avg(fct_queimada.potencia_radiativa_fogo) as media_potencia_radiativa_fogo
     from fct_queimada
     INNER JOIN public.dim_data d on d.id_data = fct_queimada.id_data
     INNER JOIN public.dim_local_queimada dlq on dlq.id_local = fct_queimada.id_local
     GROUP BY d.trimestre, dlq.regiao_uf
     )
SELECT
    jc.regiao_uf,
    jc.trimestre,
    jq.media_potencia_radiativa_fogo,
    jc.media_umidade_relativa
FROM joined_clima jc
INNER JOIN joined_queimada jq ON jc.regiao_uf = jq.regiao_uf AND jc.trimestre = jq.trimestre
