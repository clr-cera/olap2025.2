package models

case class InpeRawModel(
                         id : Long,
                         ano: Int,
                         mes: Int,
                         data_hora: java.sql.Timestamp,
                         bioma: String,
                         sigla_uf: String,
                         id_municipio: Int,
                         latitude: Double,
                         longitude: Double,
                         satelite: String,
                         dias_sem_chuva: Option[Int],
                         precipitacao: Option[Double],
                         risco_fogo: Option[Double],
                         potencia_radiativa_fogo: Option[Double],
                       )