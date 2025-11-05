package generators

import org.apache.spark.sql.Dataset

// Base trait for classes that generate tables without arguments
trait Generator[T] {
  def generate() : Dataset[T]
}
