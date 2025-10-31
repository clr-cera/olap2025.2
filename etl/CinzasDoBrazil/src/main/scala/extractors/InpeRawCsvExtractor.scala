package extractors

import config.SourceConfig
import models.InpeRawModel
import org.apache.spark.sql.Dataset
import utils.SparkSessionManager



object InpeRawCsvExtractor extends Extractor[InpeRawModel] {

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

  override def extract(options: SourceConfig): Dataset[InpeRawModel] = {
    val spark = SparkSessionManager.instance
    import spark.implicits._
    val readBuilder = spark.read.format(options.format)
      .schema(schemaDDL)
    val configuredBuilder = options.options.foldLeft(readBuilder) {
      case(b, (key, value)) => b.option(key, value)
    }
    val df = configuredBuilder.load(options.path)
    df.as[InpeRawModel]
  }
}
