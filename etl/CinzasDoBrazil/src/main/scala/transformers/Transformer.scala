package transformers

import org.apache.spark.sql.{DataFrame, Dataset}

trait Transformer[T] {
  def transform(dataset: DataFrame) : Dataset[T]
}
