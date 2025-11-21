package joiners

import org.apache.spark.sql.{DataFrame, Dataset}

// Trait for classes whose purpose is to join datasets, requires typed datasets
trait Joiner[T, U] {
  def join(dataset1: Dataset[T], dataset2: Dataset[U]): DataFrame
}
