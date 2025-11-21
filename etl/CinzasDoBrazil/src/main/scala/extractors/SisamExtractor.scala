package extractors

import config.SourceConfig
import models.SisamModel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.IntegerType
import utils.SparkSessionManager

object SisamExtractor extends Extractor[SisamModel] {

  val defaultConfig = SourceConfig(
    path = "data/sisam-full.pqt.zstd",
    format = "parquet",
    options = Map()
  )

  override def extract(options: SourceConfig): Dataset[SisamModel] = {
    val spark = SparkSessionManager.instance
    import spark.implicits._
    val readBuilder = spark.read.format(options.format)
    val configuredBuilder = options.options.foldLeft(readBuilder) {
      case (b, (key, value)) => b.option(key, value)
    }
    val df = configuredBuilder.load(options.path)
    df.withColumn("ano_temp", $"ano".cast(IntegerType))
      .withColumn("vento_direcao_temp", $"vento_direcao".cast(IntegerType))
      .drop($"ano", $"vento_direcao")
      .withColumnsRenamed(Map(
        "vento_direcao_temp" -> "vento_direcao",
        "ano_temp" -> "ano"
      ))
      .as[SisamModel]
  }
}
