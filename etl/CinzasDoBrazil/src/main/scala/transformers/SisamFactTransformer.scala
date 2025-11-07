package transformers

import models.{HorarioDimensionModel, InpeRawModel, QueimadaDateDimensionModel, QueimadaFactModel, QueimadaLocalDimensionModel, SisamFactModel, SisamModel}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._


object SisamFactTransformer {

  def transform(
                 sisamData : Dataset[SisamModel],
                 dateDim : Dataset[QueimadaDateDimensionModel],
                 localDim : Dataset[QueimadaLocalDimensionModel],
                 horarioDim : Dataset[HorarioDimensionModel]): Dataset[SisamFactModel] =
  {
    import utils.SparkSessionManager.instance.implicits._

    val sisamDateJoinExpr = sisamData("ano") === dateDim("ano") && month(sisamData("data_hora")) === dateDim("mes") && day(sisamData("data_hora")) === dateDim("dia")

    val sisamLocalJoinExpr = sisamData("id_municipio") === localDim("id_municipio") && isnull(localDim("latitude")) && isnull(localDim("longitude"))

    val sisamHorarioJoinExpr = hour(sisamData("data_hora")) === horarioDim("hora") && horarioDim("minuto") === 0

    sisamData
      .join(dateDim.withColumnRenamed("id", "data_fk"), sisamDateJoinExpr, "left")
      .join(localDim.withColumnRenamed("id", "local_fk"), sisamLocalJoinExpr, "left")
      .join(horarioDim.withColumnRenamed("id", "horario_fk"), sisamHorarioJoinExpr, "left")
      .select(
        "data_fk",
        "local_fk",
        "horario_fk",
        "temperatura",
        "precipitacao_dia",
        "umidade_relativa",
        "vento_velocidade",
        "vento_direcao",
        "co_ppb",
        "no2_ppb",
        "o3_ppb",
        "pm25_ugm3",
        "so2_ugm3"
      )
      .dropDuplicates("data_fk", "local_fk", "horario_fk")
      .as[SisamFactModel]
  }
}
