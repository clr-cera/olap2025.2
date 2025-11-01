package models

case class QueimadaDateDimensionModel(
                                  id: Long,
                                  ano: Int,
                                  semestre: Int,
                                  trimestre: Int,
                                  mes: Int,
                                  dia: Int,
                                  diaDaSemana: Int,
                                  diaDoAno: Int,
                                  numeroSemana: Int,
                                  fimDeSemana: Boolean,
                                  estacao: String
                                )

