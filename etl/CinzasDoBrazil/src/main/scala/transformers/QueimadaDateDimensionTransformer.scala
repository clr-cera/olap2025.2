package transformers

import org.apache.spark.sql.{DataFrame, Dataset}
import models.QueimadaDateDimensionModel
import org.apache.spark.sql.functions._
import utils.SparkSessionManager


object QueimadaDateDimensionTransformer extends Transformer[QueimadaDateDimensionModel] {

  override def transform(queimadas: DataFrame): Dataset[QueimadaDateDimensionModel] = {
    import SparkSessionManager.instance.implicits._
    val mapEstacao = udf((data_hora: java.sql.Timestamp) => {
      val date = data_hora.toLocalDateTime
       date.getMonth.getValue match {
        case 1 | 2 => "Verão"
        case 3 if date.getDayOfMonth >= 21 => "Outono" // Assuming equinox
        case 3 => "Verão"
        case 4 | 5 => "Outono"
        case 6 if date.getDayOfMonth >= 21 => "Inverno" // Assuming solstice
        case 6 => "Outono"
        case 7 | 8 => "Inverno"
        case 9 if date.getDayOfMonth >= 23 => "Primavera" // Assuming equinox
        case 9 => "Inverno"
        case 10 | 11 => "Primavera"
        case 12 if date.getDayOfMonth >= 22 => "Verão" // Assuming solstice
        case 12 => "Primavera"
        case _ => "Desconhecido"
      }
    })
    val dateDimensionTable = queimadas
      .select("ano", "mes", "data_hora")
      .withColumn("id", monotonically_increasing_id())
      .withColumn("semestre", when($"mes" <= 6, 1).otherwise(2))
      .withColumn("trimestre", ceil($"mes" / 4).cast("int"))
      .withColumn("dia", dayofmonth($"data_hora"))
      .withColumn("diaDaSemana", dayofweek($"data_hora"))
      .withColumn("diaDoAno", dayofyear($"data_hora"))
      .withColumn("numeroSemana", weekofyear(($"data_hora")))
      .withColumn("fimDeSemana", $"diaDaSemana".isin(1, 7))
      .withColumn("estacao", mapEstacao($"data_hora"))
      .drop("data_hora")
      .as[QueimadaDateDimensionModel]

    dateDimensionTable
  }
}
