package models

import java.sql.Timestamp

// Parquet schema
//root
//|-- ano: long (nullable = true)
//|-- sigla_uf: string (nullable = true)
//|-- id_municipio: string (nullable = true)
//|-- data_hora: timestamp_ntz (nullable = true)
//|-- co_ppb: double (nullable = true)
//|-- no2_ppb: double (nullable = true)
//|-- o3_ppb: double (nullable = true)
//|-- pm25_ugm3: double (nullable = true)
//|-- so2_ugm3: double (nullable = true)
//|-- precipitacao_dia: double (nullable = true)
//|-- temperatura: double (nullable = true)
//|-- umidade_relativa: double (nullable = true)
//|-- vento_direcao: long (nullable = true)



// Dados Brutos do Sisam
case class SisamModel(
                      ano: Option[Int],
                      sigla_uf: Option[String],
                      id_municipio: Option[String],
                      data_hora: Option[Timestamp],
                      co_ppb: Option[Double],
                      no2_ppb: Option[Double],
                      o3_ppb: Option[Double],
                      pm25_ugm3: Option[Double],
                      so2_ugm3: Option[Double],
                      precipitacao_dia: Option[Double],
                      temperatura: Option[Double],
                      umidade_relativa: Option[Double],
                      vento_direcao: Option[Int],
                      vento_velocidade: Option[Double]
                      )