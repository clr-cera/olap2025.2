CREATE MATERIALIZED VIEW drill_across_horario_mat AS
WITH joined_clima AS
         (SELECT
              dlc.id_local,
              dhc.id_horario,
              dlc.nome_municipio,
              dlc.sigla_uf,
              dhc.hora,
              avg(fct_clima.temperatura) AS media_temperature,
              avg(pm25_ugm3) AS media_pm25_ugm3
          FROM fct_clima
                   INNER JOIN public.dim_local_clima dlc ON dlc.id_local = fct_clima.id_local
                   INNER JOIN public.dim_horario_clima dhc ON dhc.id_horario = fct_clima.id_horario
          GROUP BY dlc.id_local, dhc.id_horario, dlc.nome_municipio, dlc.sigla_uf, dhc.hora
         ),
     joined_queimada AS
         (SELECT
              dhq.id_horario_clima,
              dlq.id_local_clima,
              count(*) AS count_focus
          FROM fct_queimada
                   INNER JOIN public.dim_horario_queimada dhq ON fct_queimada.id_horario = dhq.id_horario
                   INNER JOIN public.dim_local_queimada dlq ON dlq.id_local = fct_queimada.id_local
          GROUP BY dhq.id_horario_clima, dlq.id_local_clima
         )
SELECT
    jc.hora,
    (jc.nome_municipio || '/' || jc.sigla_uf) AS nome_municipio,
    jq.count_focus,
    jc.media_pm25_ugm3,
    jc.media_temperature
FROM joined_clima jc
         INNER JOIN joined_queimada jq ON jc.id_local = jq.id_local_clima AND jc.id_horario = jq.id_horario_clima;
