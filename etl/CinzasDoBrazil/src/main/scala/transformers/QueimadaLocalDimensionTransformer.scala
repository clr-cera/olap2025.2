package transformers

import models.QueimadaLocalDimensionModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

object QueimadaLocalDimensionTransformer extends Transformer[QueimadaLocalDimensionModel]{
// Builds the Local dimension table for the queimada star schema from the IBGE cities table joined with region data


  override def transform(dataset: DataFrame): Dataset[QueimadaLocalDimensionModel] = {
    import utils.SparkSessionManager.instance.implicits._
    val localDimensionTable = dataset
      .withColumn("id", monotonically_increasing_id())
      .withColumn("id_municipio", $"codigoDoMunicipioIbge")
      .withColumn("municipio", $"municipioIbge")
      .withColumn("estado", $"sigla")
      .withColumn("regiao", $"regiao")
      .select("id", "id_municipio", "municipio", "estado", "regiao")
    localDimensionTable.as[QueimadaLocalDimensionModel]
  }
}
