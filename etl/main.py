import argparse
import json
import os
import logging
from pyspark.sql import SparkSession
from etl.config import ETLConfig, SourceConfig, SourceType, SinkConfig, SinkType
from etl.extract import Extractor
from etl.transform import Transformer
from etl.load import Loader

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session(app_name: str = "Wildfire ETL") -> SparkSession:
    # Java 17+ (and especially Java 24) requires these flags for Spark to work correctly
    jvm_options = (
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
        "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
    )

    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.extraJavaOptions", jvm_options) \
        .config("spark.executor.extraJavaOptions", jvm_options) \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "2g") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0,org.postgresql:postgresql:42.7.3") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def get_default_config() -> ETLConfig:
    return ETLConfig(
        spark_config={},
        municipios_source=SourceConfig(SourceType.CSV, path="data/municipios.csv", options={"separator": ";", "encoding": "iso-8859-1"}),
        uf_source=SourceConfig(SourceType.CSV, path="data/uf.csv"),
        queimadas_source=SourceConfig(SourceType.PARQUET, path="data/queimadas-full.pqt.zstd"),
        sisam_source=SourceConfig(SourceType.PARQUET, path="data/sisam-full.pqt.zstd"),
        dim_data_sink=SinkConfig(SinkType.LOCAL_PARQUET, path="dist/dim_data"),
        dim_horarios_sink=SinkConfig(SinkType.LOCAL_PARQUET, path="dist/dim_horarios"),
        dim_local_sink=SinkConfig(SinkType.LOCAL_PARQUET, path="dist/dim_local"),
        fct_queimadas_sink=SinkConfig(SinkType.LOCAL_PARQUET, path="dist/fct_queimadas"),
        fct_clima_sink=SinkConfig(SinkType.LOCAL_PARQUET, path="dist/fct_clima")
    )

def run_etl(config: ETLConfig):
    spark = create_spark_session(config.spark_config.get("spark.app.name", "Wildfire ETL"))
    extractor = Extractor(spark)
    transformer = Transformer(spark)
    loader = Loader(spark)

    try:
        # Extract
        logger.info("Extracting data...")
        
        logger.info("Extracting municipios data...")
        municipios_df = extractor.extract(config.municipios_source)
        
        logger.info("Extracting UF data...")
        uf_df = extractor.extract(config.uf_source)
        
        logger.info("Extracting queimadas data...")
        queimadas_df = extractor.extract(config.queimadas_source)
        
        logger.info("Extracting sisam data...")
        sisam_df = extractor.extract(config.sisam_source)

        # Transform
        logger.info("Transforming data...")

        logger.info("Preprocessing queimadas data...")
        queimadas_df = transformer.preprocess_queimadas(queimadas_df)
        
        logger.info("Preprocessing clima data...")
        sisam_df = transformer.preprocess_clima(sisam_df)

        logger.info("Transforming dim_horarios...")
        dim_horarios = transformer.transform_dim_horarios()
        
        logger.info("Transforming dim_horarios_clima...")
        dim_horarios_clima = transformer.transform_dim_horarios_clima()
        
        logger.info("Transforming dim_data...")
        dim_data = transformer.transform_dim_data(queimadas_df, sisam_df)
        
        logger.info("Transforming dim_local_clima...")
        dim_local_clima = transformer.transform_dim_local_clima(sisam_df, municipios_df, uf_df)
        
        logger.info("Transforming dim_local_queimada...")
        dim_local_queimada = transformer.transform_dim_local_queimada(queimadas_df, municipios_df, uf_df)
        
        logger.info("Linking dim_local dimensions...")
        dim_local_queimada = transformer.link_dim_local_dimensions(dim_local_queimada, dim_local_clima)

        logger.info("Transforming fct_queimadas...")
        fct_queimadas = transformer.transform_fct_queimadas(queimadas_df, dim_data, dim_local_queimada, dim_horarios)
        
        logger.info("Transforming fct_clima...")
        fct_clima = transformer.transform_fct_clima(sisam_df, dim_data, dim_local_clima, dim_horarios_clima)

        # Load
        logger.info("Loading data...")
        
        # Load dimension tables first (no dependencies)
        logger.info("Loading dim_data...")
        loader.load(dim_data, config.dim_data_sink)
        
        # Load dim_horarios_clima before dim_horarios (FK dependency)
        logger.info("Loading dim_horarios_clima...")
        horarios_clima_sink = SinkConfig(
            sink_type=config.dim_horarios_sink.sink_type,
            path=f"{config.dim_horarios_sink.path}_clima" if config.dim_horarios_sink.path else None,
            connection_properties=config.dim_horarios_sink.connection_properties,
            jdbc_url=config.dim_horarios_sink.jdbc_url,
            table_name="dim_horario_clima",
            mode=config.dim_horarios_sink.mode,
        )
        loader.load(dim_horarios_clima, horarios_clima_sink)
        
        logger.info("Loading dim_horarios...")
        loader.load(dim_horarios, config.dim_horarios_sink)
        
        # Load dim_local_clima before dim_local_queimada (FK dependency)
        logger.info("Loading dim_local_clima...")
        clima_sink_config = SinkConfig(
            sink_type=config.dim_local_sink.sink_type,
            path=config.dim_local_sink.path + "_clima" if config.dim_local_sink.path else None,
            connection_properties=config.dim_local_sink.connection_properties,
            jdbc_url=config.dim_local_sink.jdbc_url,
            table_name="dim_local_clima",
            mode=config.dim_local_sink.mode
        )
        loader.load(dim_local_clima, clima_sink_config)
        
        logger.info("Loading dim_local_queimada...")
        loader.load(dim_local_queimada, config.dim_local_sink)

        # Load fact tables last (depend on all dimensions)
        logger.info("Loading fct_queimadas...")
        loader.load(fct_queimadas, config.fct_queimadas_sink)

        logger.info("Loading fct_clima...")
        loader.load(fct_clima, config.fct_clima_sink)

        logger.info("ETL finished successfully.")
        
    except Exception as e:
        logger.error(f"ETL failed: {str(e)}")
        raise
    finally:
        spark.stop()

def main():
    parser = argparse.ArgumentParser(description="Run Wildfire ETL")
    parser.add_argument("--config", type=str, help="Path to configuration JSON file", default="config.json")
    args = parser.parse_args()

    if args.config and os.path.exists(args.config):
        logger.info(f"Loading configuration from {args.config}")
        with open(args.config, 'r') as f:
            config_dict = json.load(f)
            config = ETLConfig.from_dict(config_dict)
    else:
        logger.info("Using default configuration.")
        config = get_default_config()
    
    run_etl(config)

if __name__ == "__main__":
    main()
