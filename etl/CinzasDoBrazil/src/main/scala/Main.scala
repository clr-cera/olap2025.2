import config.SourceConfig
import extractors.{IbgeMunicipiosExtractor, InpeRawExtractor}
import utils.SparkSessionManager
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import joiners._
import transformers._
import generators._
import extractors._

object Main {
  def main(args : Array[String]) : Unit = {
    import SparkSessionManager.instance.implicits._
    val inpe = InpeRawExtractor.extract(InpeRawExtractor.defaultConfig)
    val dateDimension = transformers.QueimadaDateDimensionTransformer.transform(inpe.toDF())
//    dateDimension.show(30)
    val ibge = IbgeMunicipiosExtractor.extract(IbgeMunicipiosExtractor.defaultConfig)
//    val joined = MunicipiosQueimadasJoiner.join(inpe, ibge)
//    joined.groupBy($"id").count().filter($"count" > 1).show(100)
    val uf = RegiaoExtractor.extract(RegiaoExtractor.defaultConfig)
    val regiaoJoined = RegiaoJoiner.join(ibge, uf)
    val transLocalDim = QueimadaLocalDimensionTransformer.transform(regiaoJoined)
    val localInpeJoined = LocalInpeJoiner.join(inpe, transLocalDim)
    val pointDim = QueimadaPointDimensionTransformer.transform(localInpeJoined)
    val horarioDim = HorarioDimensionGenerator.generate()
//    horarioDim.show(50, false)
    val queimadaFact = QueimadaFactTransformer.transform(
      inpe,
      dateDimension,
      pointDim,
      horarioDim
    )
    queimadaFact.show(80, false)
    SparkSessionManager.instance.stop()
  }
}
