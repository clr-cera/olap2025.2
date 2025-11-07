package transformers

import models.{IbgeMunicipioModel, InpeRawModel, QueimadaLocalDimensionModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object QueimadaLocalDimensionTransformer extends Transformer[QueimadaLocalDimensionModel]{
// Builds the Local dimension table for the queimada star schema from the INPE table joined with IBGE cities table joined with region data


  def joinInpeIbgeRegiao(inpe: Dataset[InpeRawModel], ibge: Dataset[IbgeMunicipioModel], regioes: Dataset[Row]): DataFrame = {
    import utils.SparkSessionManager.instance.implicits._
    val projectedUfDf = regioes.select("sigla", "regiao")
    inpe.join(ibge, inpe("id_municipio") === ibge("codigoDoMunicipioIbge"), "left")
      .join(regioes, $"uf" === regioes("sigla"), "left")
  }


  override def transform(dataset: DataFrame): Dataset[QueimadaLocalDimensionModel] = {
    import utils.SparkSessionManager.instance.implicits._
    val localDimensionTable = dataset
      .withColumn("id", monotonically_increasing_id())
      .withColumn("id_municipio", $"codigoDoMunicipioIbge")
      .withColumn("municipio", $"municipioTom")
      .withColumn("estado", $"uf")
      .withColumn("regiao", $"regiao")
      .withColumn("latitude", $"latitude")
      .withColumn("longitude", $"longitude")
      .withColumn("bioma", $"bioma")
      .select("id", "id_municipio", "municipio", "estado", "regiao", "latitude", "longitude", "bioma")
      .filter(not(isnull($"id_municipio"))) // O do sisam tinha umas esquisitices
      .dropDuplicates("id_municipio", "latitude", "longitude")
    localDimensionTable.as[QueimadaLocalDimensionModel].filter(_.id_municipio != 0)
  }
}
