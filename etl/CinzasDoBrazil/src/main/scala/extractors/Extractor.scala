package extractors

import config.SourceConfig
import org.apache.spark.sql.Dataset


// Trait for extractors, receives a SourceConfig for details of source and must return a strongly typed dataset
trait Extractor[T] {
  def extract(options: SourceConfig): Dataset[T]
}
