package extractors

import config.SourceConfig
import models.IbgeMunicipioModel
import org.apache.spark.sql.Dataset
import utils.SparkSessionManager


// Extracts data for municipal names and codes
object IbgeMunicipiosExtractor extends Extractor[IbgeMunicipioModel] {

  private val schemaDDL =
    """`codigoDoMunicipioTom` BIGINT,
      |`codigoDoMunicipioIbge` BIGINT,
      |`municipioTom` STRING,
      |`municipioIbge` STRING,
      |`uf` STRING""".stripMargin

//  Only for ease of use, may be changed later, copy this code if needed
  val defaultConfig = SourceConfig(
    path = "data/municipios.csv",
    format = "csv",
    options = Map("sep" -> ";", "header" -> "true", "inferSchema" -> "false", "encoding" -> "iso-8859-1")
  )

  override def extract(options: SourceConfig): Dataset[IbgeMunicipioModel] = {
    val spark = SparkSessionManager.instance
    import spark.implicits._
    val readBuilder = spark.read.format(options.format)
      .schema(schemaDDL)
    val configuredBuilder = options.options.foldLeft(readBuilder) {
      case(b, (key, value)) => b.option(key, value)
    }
    val df = configuredBuilder.load(options.path)
    df.as[IbgeMunicipioModel]
  }
}
