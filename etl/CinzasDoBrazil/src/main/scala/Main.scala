import config.SourceConfig
import extractors.{IbgeMunicipiosExtractor, InpeRawCsvExtractor}
import utils.SparkSessionManager
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import joiners._

object Main {
  def main(args : Array[String]) : Unit = {
    import SparkSessionManager.instance.implicits._
    val inpe = InpeRawCsvExtractor.extract(InpeRawCsvExtractor.defaultConfig)
    val ibge = IbgeMunicipiosExtractor.extract(IbgeMunicipiosExtractor.defaultConfig)
    val joined = MunicipiosQueimadasJoiner.join(inpe, ibge)
    joined.groupBy($"id").count().filter($"count" > 1).show(100)
    SparkSessionManager.instance.stop()
  }
}
