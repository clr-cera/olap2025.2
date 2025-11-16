package extractors

import config.SourceConfig
import models.InpeRawModel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import utils.SparkSessionManager


// Extracts raw fire data from csv format
object InpeRawExtractor extends Extractor[InpeRawModel] {

  val schemaDDL = """
  ano LONG,
  mes LONG,
  data_hora TIMESTAMP,
  bioma STRING,
  sigla_uf STRING,
  id_municipio BINARY,
  latitude DOUBLE,
  longitude DOUBLE,
  satelite STRING,
  dias_sem_chuva DOUBLE,
  precipitacao DOUBLE,
  risco_fogo DOUBLE,
  potencia_radiativa_fogo DOUBLE
  """

  //  Only for ease of use, may be changed later, copy this code if needed
  val defaultConfig = SourceConfig(
    path = "data/queimadas-full.pqt.zstd",
    format = "parquet",
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
      .withColumn("ano", col("ano").cast(IntegerType))
      .withColumn("mes",col("mes").cast(IntegerType))
      .withColumn("dias_sem_chuva",col("dias_sem_chuva").cast(IntegerType))
      .withColumn("id_municipio", col("id_municipio").cast(StringType).cast(IntegerType))
      .as[InpeRawModel]
  }
}
