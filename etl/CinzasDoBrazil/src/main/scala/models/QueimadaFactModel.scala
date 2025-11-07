package models

case class QueimadaFactModel(
                            data_fk : Long,
                            local_fk : Long,
                            horario_fk : Long,
                            risco_fogo : Option[Double],
                            frp : Option[Double],
                            dias_sem_chuva : Option[Int]
                            )
