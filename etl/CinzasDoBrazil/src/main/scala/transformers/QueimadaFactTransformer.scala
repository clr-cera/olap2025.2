package transformers

import models.{HorarioDimensionModel, InpeRawModel, QueimadaDateDimensionModel, QueimadaFactModel, QueimadaLocalDimensionModel, QueimadaPointDimensionModel}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

object QueimadaFactTransformer {
  def transform(
                 inpeData : Dataset[InpeRawModel],
                 dateDim : Dataset[QueimadaDateDimensionModel],
                 pointDim : Dataset[QueimadaPointDimensionModel],
                 horarioDim : Dataset[HorarioDimensionModel]): Dataset[QueimadaFactModel] =
  {
    import utils.SparkSessionManager.instance.implicits._
    val inpeDateJoinExpr = inpeData("ano") === dateDim("ano") && inpeData("mes") === dateDim("mes") && day(inpeData("data_hora")) === dateDim("dia")
    val inpeLocalJoinExpr = inpeData("latitude") === pointDim("latitude") && inpeData("longitude") === pointDim("longitude")
    val inpeHorarioJoinExpr = hour($"data_hora") === horarioDim("hora") && minute($"data_hora") === horarioDim("minuto")
    inpeData.withColumnsRenamed(Map("potencia_radiativa_fogo" -> "frp"))
      .join(dateDim.withColumnRenamed("id", "data_fk"), inpeDateJoinExpr, "left")
      .join(pointDim.withColumnRenamed("id", "ponto_fk"), inpeLocalJoinExpr, "left")
      .join(horarioDim.withColumnRenamed("id", "horario_fk"), inpeHorarioJoinExpr, "left")
      .select("data_fk", "ponto_fk", "horario_fk", "risco_fogo", "frp", "dias_sem_chuva")
      .dropDuplicates("data_fk", "ponto_fk", "horario_fk")
      .as[QueimadaFactModel]
  }
}
