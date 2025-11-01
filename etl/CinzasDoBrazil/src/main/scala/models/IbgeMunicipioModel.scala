package models


// Represents data from IBGE brazilian cities table
case class IbgeMunicipioModel(
                      codigoDoMunicipioTom: Long,
                      codigoDoMunicipioIbge: Long,
                      municipioTom: String,
                      municipioIbge: String,
                      uf: String
                    )
