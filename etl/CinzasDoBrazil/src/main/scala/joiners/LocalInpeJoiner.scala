package joiners

import models.{InpeRawModel, QueimadaLocalDimensionModel}
import org.apache.spark.sql.{DataFrame, Dataset}

// Joins the inpe data to the Local dimension for generating the Ponto dimension
object LocalInpeJoiner extends Joiner[InpeRawModel, QueimadaLocalDimensionModel] {

  override def join(inpeDs: Dataset[InpeRawModel], localDimDs: Dataset[QueimadaLocalDimensionModel]): DataFrame = {
    import utils.SparkSessionManager.instance.implicits._
    inpeDs
      .withColumnsRenamed(Map("id"-> "inpe_id", "id_municipio" -> "id_municipio_inpe"))
      .select("inpe_id", "id_municipio_inpe", "latitude", "longitude", "bioma")
      .join(localDimDs.withColumnRenamed("id", "local_id"), $"id_municipio_inpe" === $"id_municipio", "left")
      .drop("id_municipio_inpe")
  }
}
