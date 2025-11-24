from pyspark.sql import SparkSession, DataFrame
from etl.config import SourceConfig, SourceType

class Extractor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def extract(self, config: SourceConfig) -> DataFrame:
        if config.source_type == SourceType.CSV:
            reader = self.spark.read.format("csv").option("header", "true").option("inferSchema", "true")
            if config.options:
                for k, v in config.options.items():
                    reader = reader.option(k, v)
            return reader.load(config.path)
            
        elif config.source_type == SourceType.PARQUET:
            return self.spark.read.parquet(config.path)
            
        elif config.source_type == SourceType.BIGQUERY:
            # Assumes spark-bigquery-connector is available
            return self.spark.read.format("bigquery").option("table", config.table).load()
            
        else:
            raise ValueError(f"Unsupported source type: {config.source_type}")
