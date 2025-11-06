
import utils.SparkSessionManager
import jobs.InpeETLPipeline
import extractors._

import java.util.Properties

object Main {
  def main(args : Array[String]) : Unit = {
    val pipeline = new InpeETLPipeline(
      InpeRawExtractor.defaultConfig,
      IbgeMunicipiosExtractor.defaultConfig,
      RegiaoExtractor.defaultConfig
    )
//    val inpe = InpeRawExtractor.extract(InpeRawExtractor.defaultConfig)
//    println(s"NUMERO DE LINHAS = ${inpe.count()}")
    pipeline.executePipeline()
    SparkSessionManager.instance.stop()
  }
}
