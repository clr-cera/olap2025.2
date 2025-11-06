package transformers

import models.QueimadaPointDimensionModel
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._


// Generates Ponto dimension table from the dataframe joined from Local Dimension and the inpe dataset
object QueimadaPointDimensionTransformer extends Transformer [QueimadaPointDimensionModel]{

  override def transform(inpeLocalJoinedDf: DataFrame): Dataset[QueimadaPointDimensionModel] = {
    import utils.SparkSessionManager.instance.implicits._
    inpeLocalJoinedDf
      .withColumn("id", monotonically_increasing_id())
      .withColumn("local_fk", $"local_id")
      .withColumn("latitude", $"latitude")
      .withColumn("longitude", $"longitude")
      .withColumn("bioma", $"bioma")
      .select("id", "local_fk", "latitude", "longitude", "bioma")
      .dropDuplicates("latitude", "longitude")
      .as[QueimadaPointDimensionModel]
  }
}
