package jobs

import config.SourceConfig
import extractors.{IbgeMunicipiosExtractor, InpeRawExtractor, RegiaoExtractor}
import generators.HorarioDimensionGenerator
import joiners.{LocalInpeJoiner, RegiaoJoiner}
import loaders.QueimadaSchemaLoader
import transformers.{QueimadaFactTransformer, QueimadaLocalDimensionTransformer, QueimadaPointDimensionTransformer}

class InpeETLPipeline(val inpeSrc : SourceConfig, val ibgeCitiesSrc : SourceConfig, val regionSrc : SourceConfig)
{
  def executePipeline() = {

    val inpe = InpeRawExtractor.extract(inpeSrc)
    val ibge = IbgeMunicipiosExtractor.extract(ibgeCitiesSrc)
    val uf = RegiaoExtractor.extract(regionSrc)

    val dateDimension = transformers.QueimadaDateDimensionTransformer.transform(inpe.toDF())

    val regiaoJoined = RegiaoJoiner.join(ibge, uf)

    val transLocalDim = QueimadaLocalDimensionTransformer.transform(regiaoJoined)

    val localInpeJoined = LocalInpeJoiner.join(inpe, transLocalDim)

    val pointDim = QueimadaPointDimensionTransformer.transform(localInpeJoined)

    val horarioDim = HorarioDimensionGenerator.generate()

    val queimadaFact = QueimadaFactTransformer.transform(
      inpe,
      dateDimension,
      pointDim,
      horarioDim
    )

    QueimadaSchemaLoader.load(
      dateDimension,
      horarioDim,
      pointDim,
      transLocalDim,
      queimadaFact
    )

  }
}
