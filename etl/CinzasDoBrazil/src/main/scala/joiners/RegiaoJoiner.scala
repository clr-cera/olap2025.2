package joiners


import models.IbgeMunicipioModel
import org.apache.spark.sql.{DataFrame, Dataset, Row}

// Joins ibge cities dataset to region data
object RegiaoJoiner extends Joiner[IbgeMunicipioModel, Row] {

  override def join(ibgeDf: Dataset[IbgeMunicipioModel], ufDf: Dataset[Row]): DataFrame = {
    import utils.SparkSessionManager.instance.implicits._
    val projectedUfDf = ufDf.select("sigla", "regiao")
    val joinedDf = ibgeDf.join(projectedUfDf, $"uf" === $"sigla", "left")
    joinedDf
  }
}
