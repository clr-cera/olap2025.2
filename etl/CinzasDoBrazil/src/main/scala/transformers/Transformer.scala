package transformers

import org.apache.spark.sql.{DataFrame, Dataset}


// Base trait for transformations, must return a typed dataset
trait Transformer[T] {
  def transform(dataset: DataFrame): Dataset[T]
}
