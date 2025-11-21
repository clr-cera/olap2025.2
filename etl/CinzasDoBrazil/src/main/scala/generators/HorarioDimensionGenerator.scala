package generators

import models.HorarioDimensionModel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

object HorarioDimensionGenerator extends Generator[HorarioDimensionModel] {

  override def generate(): Dataset[HorarioDimensionModel] = {
    import utils.SparkSessionManager.instance.implicits._
    val hours = 0 until 24
    val minutes = 0 until 60
    val combinations = for {
      hour <- hours
      minute <- minutes
    } yield {
      (hour, minute)
    }
    combinations
      .toDF("hora", "minuto")
      .withColumn("id", monotonically_increasing_id())
      .as[HorarioDimensionModel]
  }
}
