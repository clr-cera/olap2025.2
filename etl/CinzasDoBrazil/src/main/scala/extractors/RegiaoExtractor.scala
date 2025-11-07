package extractors

import config.SourceConfig
import extractors.InpeRawExtractor.schemaDDL
import models.InpeRawModel
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import utils.SparkSessionManager

object RegiaoExtractor extends Extractor[Row] {

  val defaultConfig = SourceConfig(
    path = "data/uf.csv",
    format = "csv",
    options = Map("header" -> "true")
  )

  override def extract(options: SourceConfig): Dataset[Row] = {
    val spark = SparkSessionManager.instance
    import spark.implicits._
    val readBuilder = spark.read.format(options.format)
      .option("inferSchema", "true")
    val configuredBuilder = options.options.foldLeft(readBuilder) {
      case(b, (key, value)) => b.option(key, value)
    }
    val df = configuredBuilder.load(options.path)
    df
  }
}
