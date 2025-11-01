package joiners

import models.{IbgeMunicipioModel, InpeRawModel}
import org.apache.spark.sql.{DataFrame, Dataset}
import utils.SparkSessionManager


object MunicipiosQueimadasJoiner extends Joiner[InpeRawModel, IbgeMunicipioModel]{

  override def join(queimadas: Dataset[InpeRawModel], municipios: Dataset[IbgeMunicipioModel]): DataFrame = {
    import SparkSessionManager.instance.implicits._
    val projectedMunicipios = municipios.select("codigoDoMunicipioIbge", "municipioIbge")
    val joined = queimadas.join(projectedMunicipios, $"id_municipio" === $"codigoDoMunicipioIbge", "left")
    joined.drop("codigoDoMunicipioIbge")
  }
}
