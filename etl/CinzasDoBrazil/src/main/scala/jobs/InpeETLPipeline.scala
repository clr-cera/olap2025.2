package jobs

import config.SourceConfig
import extractors.{IbgeMunicipiosExtractor, InpeRawExtractor, RegiaoExtractor, SisamExtractor}
import generators.HorarioDimensionGenerator
import joiners.{LocalInpeJoiner, RegiaoJoiner}
import loaders.QueimadaSchemaLoader
import models.{InpeRawModel, SisamModel}
import org.apache.spark.sql.catalyst.expressions.DateAdd
import org.apache.spark.sql.{DataFrame, Dataset}
import transformers.{QueimadaDateDimensionTransformer, QueimadaFactTransformer, QueimadaLocalDimensionTransformer, SisamFactTransformer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType}

class InpeETLPipeline(val inpeSrc : SourceConfig, val ibgeCitiesSrc : SourceConfig, val regionSrc : SourceConfig, sisamSrc : SourceConfig)
{
  import utils.SparkSessionManager.instance.implicits._
  lazy val inpeDs = InpeRawExtractor.extract(inpeSrc).filter($"ano".between(2013, 2015))

  lazy val ibgeDs = IbgeMunicipiosExtractor.extract(ibgeCitiesSrc)

  lazy val ufDf = RegiaoExtractor.extract(regionSrc)

  lazy val sisamDs =  SisamExtractor.extract(sisamSrc).filter($"ano".between(2013, 2015))


  def unionAndSelectDate(): DataFrame = {
    inpeDs.select("ano", "mes", "data_hora")
      .union(
        sisamDs.withColumn("mes", month($"data_hora"))
          .select("ano", "mes", "data_hora")
      )
  }

  def joinRegion(df : DataFrame): DataFrame = {
    val projectedUfDf = ufDf.select("sigla", "regiao")
    df.join(ibgeDs, df("id_municipio") === ibgeDs("codigoDoMunicipioIbge"), "left")
      .join(ufDf, $"uf" === ufDf("sigla"), "left")
  }

  def unionAndSelectLocal(joinedInpe : DataFrame, joinedSisam : DataFrame): DataFrame = {
    joinedInpe.select("codigoDoMunicipioIbge", "municipioTom", "uf", "regiao", "latitude", "longitude", "bioma")
      .union(
        joinedSisam.withColumns(Map(
            "latitude" -> lit(null).cast(DoubleType),
            "longitude" -> lit(null).cast(DoubleType),
            "bioma" -> lit(null).cast(StringType)
          ))
          .select("codigoDoMunicipioIbge", "municipioTom", "uf", "regiao", "latitude", "longitude", "bioma")
      )
  }

  def executePipelineNew() = {



      val dateDimension =  QueimadaDateDimensionTransformer.transform(unionAndSelectDate())
      scala.io.StdIn.readLine()
      val localDimension = QueimadaLocalDimensionTransformer.transform(unionAndSelectLocal(joinRegion(inpeDs.toDF()), joinRegion(sisamDs.toDF())))

      val horarioDimension = HorarioDimensionGenerator.generate()

      val queimadaFact = QueimadaFactTransformer.transform(
        inpeDs,
        dateDimension,
        localDimension,
        horarioDimension
      )

      val sisamFact = SisamFactTransformer.transform(
        sisamDs,
        dateDimension,
        localDimension,
        horarioDimension
      )

    println(s"NUMERO DE LINHAS SISAM ORIGINAL: ${sisamDs.count()}")
    println(s"NUMERO DE LINHAS SISAM FATOS: ${sisamFact.count()}")
    sisamFact.show(100, false)

  }

  def executePipeline() = {
//
//    val df = InpeRawExtractor.extract(inpeSrc)
//    val ibge = IbgeMunicipiosExtractor.extract(ibgeCitiesSrc)
//    val uf = RegiaoExtractor.extract(regionSrc)
//
//    val dateDimension = transformers.QueimadaDateDimensionTransformer.transform(inpe.toDF())
//
//    val regiaoJoined = QueimadaLocalDimensionTransformer.joinInpeIbgeRegiao(inpe, ibge, uf)
//
//    val localDim = QueimadaLocalDimensionTransformer.transform(regiaoJoined)
//
//    val localInpeJoined = LocalInpeJoiner.join(inpe, localDim)
//
//
//
//
//    QueimadaSchemaLoader.load(
//      dateDimension,
//      horarioDim,
//      localDim,
//      queimadaFact
//    )
//
  }
}
