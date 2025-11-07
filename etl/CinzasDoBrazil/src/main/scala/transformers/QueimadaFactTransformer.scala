package transformers

import models.{HorarioDimensionModel, InpeRawModel, QueimadaDateDimensionModel, QueimadaFactModel, QueimadaLocalDimensionModel}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

object QueimadaFactTransformer {
  def transform(
                 inpeData : Dataset[InpeRawModel],
                 dateDim : Dataset[QueimadaDateDimensionModel],
                 localDim : Dataset[QueimadaLocalDimensionModel],
                 horarioDim : Dataset[HorarioDimensionModel]): Dataset[QueimadaFactModel] =
  {
    import utils.SparkSessionManager.instance.implicits._
    val inpeDateJoinExpr = inpeData("ano") === dateDim("ano") && inpeData("mes") === dateDim("mes") && day(inpeData("data_hora")) === dateDim("dia")
    val inpeLocalJoinExpr = inpeData("latitude") === localDim("latitude") && inpeData("longitude") === localDim("longitude")
    val inpeHorarioJoinExpr = hour($"data_hora") === horarioDim("hora") && minute($"data_hora") === horarioDim("minuto")
    inpeData.withColumnsRenamed(Map("potencia_radiativa_fogo" -> "frp"))
      .join(dateDim.withColumnRenamed("id", "data_fk"), inpeDateJoinExpr, "left")
      .join(localDim.withColumnRenamed("id", "local_fk"), inpeLocalJoinExpr, "left")
      .join(horarioDim.withColumnRenamed("id", "horario_fk"), inpeHorarioJoinExpr, "left")
      .select("data_fk", "local_fk", "horario_fk", "risco_fogo", "frp", "dias_sem_chuva")
      .dropDuplicates("data_fk", "local_fk", "horario_fk")
      .as[QueimadaFactModel]
  }
}
