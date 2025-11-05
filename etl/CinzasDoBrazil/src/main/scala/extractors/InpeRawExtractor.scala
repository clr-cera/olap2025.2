package extractors

import config.SourceConfig
import models.InpeRawModel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import utils.SparkSessionManager


// Extracts raw fire data from csv format
object InpeRawExtractor extends Extractor[InpeRawModel] {

  val schemaDDL = """
  ano INT,
  mes INT,
  data_hora TIMESTAMP,
  bioma STRING,
  sigla_uf STRING,
  id_municipio INT,
  latitude DOUBLE,
  longitude DOUBLE,
  satelite STRING,
  dias_sem_chuva INT,
  precipitacao DOUBLE,
  risco_fogo DOUBLE,
  potencia_radiativa_fogo DOUBLE
  """

  //  Only for ease of use, may be changed later, copy this code if needed
  val defaultConfig = SourceConfig(
    path = "data/INPE_09_2024.csv",
    format = "csv",
    options = Map("header" -> "true")
  )

  override def extract(options: SourceConfig): Dataset[InpeRawModel] = {
    val spark = SparkSessionManager.instance
    import spark.implicits._
    val readBuilder = spark.read.format(options.format)
      .schema(schemaDDL)
      .option("inferSchema", "false")
    val configuredBuilder = options.options.foldLeft(readBuilder) {
      case(b, (key, value)) => b.option(key, value)
    }
    val df = configuredBuilder.load(options.path)
    df
      .withColumn("id", monotonically_increasing_id())
      .as[InpeRawModel]
  }
}
