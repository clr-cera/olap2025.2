package models

case class QueimadaPointDimensionModel(
                                      id : Long,
                                      local_fk : Long,
                                      latitude : Double,
                                      longitude : Double,
                                      bioma : String
                                      )
