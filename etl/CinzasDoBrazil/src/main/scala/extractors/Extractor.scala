package extractors

import config.SourceConfig
import org.apache.spark.sql.Dataset

trait Extractor[T] {
  def extract(options: SourceConfig): Dataset[T]
}
