package jobs

import config.SourceConfig
import extractors.SisamExtractor
import models.{HorarioDimensionModel, IbgeMunicipioModel, QueimadaDateDimensionModel, QueimadaLocalDimensionModel}
import org.apache.spark.sql.{DataFrame, Dataset}

class SisamETLPipeline(
                      sisamSrc : SourceConfig,
                      ibgeDataset : Dataset[IbgeMunicipioModel],
                      regiaoDf : DataFrame,
                      queimadaLocalDimension : Dataset[QueimadaLocalDimensionModel],
                      queimadaDateDimension : Dataset[QueimadaDateDimensionModel],
                      queimadaHorarioDimension : Dataset[HorarioDimensionModel]
                      )
{

  def execute() = {
    // Extracts sisam dataset
    val sisamDataset = SisamExtractor.extract(sisamSrc)

    // Joins with local and region data

  }


}
