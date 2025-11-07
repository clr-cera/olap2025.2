package models

case class QueimadaLocalDimensionModel(
                                      id : Long,
                                      municipio : String,
                                      id_municipio : Long,
                                      estado : String,
                                      regiao : String,
                                      latitude : Option[Double],
                                      longitude : Option[Double],
                                      bioma : Option[String]
                                      )

