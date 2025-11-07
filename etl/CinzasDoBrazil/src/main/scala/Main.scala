
import utils.SparkSessionManager
import jobs.InpeETLPipeline
import extractors._

import java.util.Properties

object Main {


  def inpeJob() = {
    val pipeline = new InpeETLPipeline(
      InpeRawExtractor.defaultConfig,
      IbgeMunicipiosExtractor.defaultConfig,
      RegiaoExtractor.defaultConfig,
      SisamExtractor.defaultConfig
    )
    //    val inpe = InpeRawExtractor.extract(InpeRawExtractor.defaultConfig)
    //    println(s"NUMERO DE LINHAS = ${inpe.count()}")
    pipeline.executePipelineNew()
  }

  def sisamTest() = {
    val sisam = SisamExtractor.extract(SisamExtractor.defaultConfig)
    sisam.show(100, false)
  }


  def main(args : Array[String]) : Unit = {
    inpeJob()
    SparkSessionManager.instance.stop()
  }
}
