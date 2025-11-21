
import extractors._
import jobs.InpeETLPipeline
import utils.SparkSessionManager

object Main {

  def main(args: Array[String]): Unit = {
    inpeJob()
    SparkSessionManager.instance.stop()
  }

  def inpeJob() = {
    val pipeline = new InpeETLPipeline(
      InpeRawExtractor.defaultConfig,
      IbgeMunicipiosExtractor.defaultConfig,
      RegiaoExtractor.defaultConfig,
      SisamExtractor.defaultConfig
    )

    pipeline.executePipelineNew()
  }
}
