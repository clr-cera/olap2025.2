package extractors

import config.SourceConfig
import org.apache.spark.sql.{Dataset, Row}
import utils.SparkSessionManager

object RegiaoExtractor extends Extractor[Row] {

  val defaultConfig = SourceConfig(
    path = "data/uf.csv",
    format = "csv",
    options = Map("header" -> "true")
  )

  override def extract(options: SourceConfig): Dataset[Row] = {
    val spark = SparkSessionManager.instance
    val readBuilder = spark.read.format(options.format)
      .option("inferSchema", "true")
    val configuredBuilder = options.options.foldLeft(readBuilder) {
      case (b, (key, value)) => b.option(key, value)
    }
    val df = configuredBuilder.load(options.path)
    df
  }
}
