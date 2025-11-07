package models

case class SisamFactModel(
                      // --- Chaves Estrangeiras (Dimensões) ---
                      data_fk: Long,
                      local_fk: Long,
                      horario_fk: Long,

                      // --- Fatos (Métricas) ---
                      // Todas as métricas são 'Option[Double]' para permitir valores nulos (nullable)
                      temperatura: Option[Double],
                      precipitacao_dia: Option[Double],
                      umidade_relativa: Option[Double],
                      vento_velocidade: Option[Double],
                      vento_direcao: Option[Int],
                      co_ppb: Option[Double],
                      no2_ppb: Option[Double],
                      o3_ppb: Option[Double],
                      pm25_ugm3: Option[Double],
                      so2_ugm3: Option[Double]
                    )
