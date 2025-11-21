package models


// Represents the date dimension to be included in the final star schema
case class QueimadaDateDimensionModel(
                                       id: Long,
                                       ano: Int,
                                       semestre: Int,
                                       trimestre: Int,
                                       mes: Int,
                                       dia: Int,
                                       dia_da_semana: Int,
                                       dia_do_ano: Int,
                                       numero_semana: Int,
                                       fim_de_semana: Boolean,
                                       estacao: String
                                     )

