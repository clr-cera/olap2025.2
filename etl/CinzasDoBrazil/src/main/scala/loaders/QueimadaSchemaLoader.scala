package loaders
import com.typesafe.config.ConfigFactory
import models._
import org.apache.spark.sql.{Dataset, SaveMode}
import utils.SparkSessionManager

import java.util.Properties


object QueimadaSchemaLoader {
  lazy val cfg = ConfigFactory.load()

  def load(
          dimData : Dataset[QueimadaDateDimensionModel],
          dimHorario : Dataset[HorarioDimensionModel],
          dimLocal : Dataset[QueimadaLocalDimensionModel],
          fctQueimada : Dataset[QueimadaFactModel]
          ) : Unit =
    {
      val spark = SparkSessionManager.instance
      import spark.implicits._

      val connectionProperties = new Properties()
      connectionProperties.put("user", cfg.getString("database.user"))
      connectionProperties.put("password", cfg.getString("database.password"))
      connectionProperties.put("driver", cfg.getString("database.driver"))

      val jdbcUrl = cfg.getString("database.url")

      dimData.write
        .mode(SaveMode.Append)
        .jdbc(jdbcUrl, "etl_result.dim_data", connectionProperties)

      dimLocal.write
        .mode(SaveMode.Append)
        .jdbc(jdbcUrl, "etl_result.dim_local", connectionProperties)


      dimHorario.write
        .mode(SaveMode.Append)
        .jdbc(jdbcUrl, "etl_result.dim_horario", connectionProperties)

      fctQueimada.write
        .mode(SaveMode.Append)
        .jdbc(jdbcUrl, "etl_result.fct_queimada", connectionProperties)

    }
}
