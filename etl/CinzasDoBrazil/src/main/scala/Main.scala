import config.SourceConfig
import extractors.InpeRawCsvExtractor
import utils.SparkSessionManager

object Main {
  def main(args : Array[String]) = {
    val ds = InpeRawCsvExtractor.extract(SourceConfig(
      path = getClass.getResource("INPE_09_2024.csv").getPath,
      format = "csv",
      options = Map("header" -> "true")
    ))
    SparkSessionManager.instance.stop()
  }
}
