package joiners

import org.apache.spark.sql.{DataFrame, Dataset}

trait Joiner[T, U] {
  def join(dataset1 : Dataset[T], dataset2 : Dataset[U]) : DataFrame
}
