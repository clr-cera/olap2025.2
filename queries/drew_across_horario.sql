CREATE MATERIALIZED VIEW drill_across_horario_mat AS
WITH joined_clima AS
         (SELECT
              dlc.id_municipio,
              dlc.nome_municipio,
              dlc.sigla_uf,
              dhc.hora,
              avg(fct_clima.temperatura) as media_temperature,
              avg(pm25_ugm3) as media_pm25_ugm3
          FROM fct_clima
                   INNER JOIN public.dim_local_clima dlc on dlc.id_local = fct_clima.id_local
                   INNER JOIN public.dim_horario_clima dhc on dhc.id_horario = fct_clima.id_horario
          GROUP BY dlc.id_municipio, dlc.nome_municipio, dlc.sigla_uf, dhc.hora
         ),
     joined_queimada AS
         (SELECT
              dhq.hora,
              dlq.id_municipio,
              dlq.nome_municipio,
              dlq.sigla_uf,
              Count(*) as count_focus
          from fct_queimada
                   INNER JOIN public.dim_horario_queimada dhq on fct_queimada.id_horario = dhq.id_horario
                   INNER JOIN public.dim_local_queimada dlq on dlq.id_local = fct_queimada.id_local
          GROUP BY dhq.hora, dlq.id_municipio, dlq.nome_municipio, dlq.sigla_uf
         )
SELECT
    jc.hora,
    (jc.nome_municipio || '/' || jc.sigla_uf) AS nome_municipio,
    jq.count_focus,
    jc.media_pm25_ugm3,
    jc.media_temperature
FROM joined_clima jc
         INNER JOIN joined_queimada jq ON jc.hora = jq.hora AND jc.id_municipio = jq.id_municipio;
