package jobs

import config.SourceConfig
import extractors.{IbgeMunicipiosExtractor, InpeRawExtractor, RegiaoExtractor}
import generators.HorarioDimensionGenerator
import joiners.{LocalInpeJoiner, RegiaoJoiner}
import loaders.QueimadaSchemaLoader
import transformers.{QueimadaFactTransformer, QueimadaLocalDimensionTransformer}

class InpeETLPipeline(val inpeSrc : SourceConfig, val ibgeCitiesSrc : SourceConfig, val regionSrc : SourceConfig)
{
  def executePipeline() = {

    val inpe = InpeRawExtractor.extract(inpeSrc)
    val ibge = IbgeMunicipiosExtractor.extract(ibgeCitiesSrc)
    val uf = RegiaoExtractor.extract(regionSrc)

    val dateDimension = transformers.QueimadaDateDimensionTransformer.transform(inpe.toDF())

    val regiaoJoined = QueimadaLocalDimensionTransformer.joinInpeIbgeRegiao(inpe, ibge, uf)

    val localDim = QueimadaLocalDimensionTransformer.transform(regiaoJoined)

    val localInpeJoined = LocalInpeJoiner.join(inpe, localDim)

    val horarioDim = HorarioDimensionGenerator.generate()

    val queimadaFact = QueimadaFactTransformer.transform(
      inpe,
      dateDimension,
      localDim,
      horarioDim
    )

    QueimadaSchemaLoader.load(
      dateDimension,
      horarioDim,
      localDim,
      queimadaFact
    )

  }
}
