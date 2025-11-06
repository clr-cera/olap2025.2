import config.SourceConfig
import extractors.{IbgeMunicipiosExtractor, InpeRawExtractor}
import utils.SparkSessionManager
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import joiners._
import transformers._
import generators._
import extractors._
import loaders._

import java.util.Properties

object Main {



  def pipeline() = {
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
    //    queimadaFact.show(80, false)
    QueimadaSchemaLoader.load(
      dimData = dateDimension, dimHorario = horarioDim, dimPonto = pointDim, dimLocal = transLocalDim, fctQueimada = queimadaFact
    )

  }

  def test() = {
    val ibge = IbgeMunicipiosExtractor.extract(IbgeMunicipiosExtractor.defaultConfig)
    import SparkSessionManager.instance.implicits._
//    ibge.select(ibge.columns.map(c =>
//      count(when(isnull(col(c)), col(c))).alias(c)
//    ): _*).show()
    val uf = RegiaoExtractor.extract(RegiaoExtractor.defaultConfig)
    val regiaoJoined = RegiaoJoiner.join(ibge, uf)
    val transLocalDim = QueimadaLocalDimensionTransformer.transform(regiaoJoined)

    transLocalDim.filter(_.id_municipio < 1000000).show()

//    uf.select(uf.columns.map(c =>
//      count(when(isnull(col(c)), col(c))).alias(c)
//    ): _*).show()
//    regiaoJoined.select(regiaoJoined.columns.map(c =>
//      count(when(isnull(col(c)), col(c))).alias(c)
//    ): _*).show()
//    transLocalDim.select(transLocalDim.columns.map(c =>
//      count(when(isnull(col(c)), col(c))).alias(c)
//    ): _*).show()
  }


  def main(args : Array[String]) : Unit = {
    import SparkSessionManager.instance.implicits._
//    test()
    pipeline()
    SparkSessionManager.instance.stop()
  }
}
