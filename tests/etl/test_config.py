import pytest
from etl.config import ETLConfig, SourceType, SinkType

def test_config_from_dict():
    config_dict = {
        "spark_config": {"spark.app.name": "Test"},
        "municipios_source": {"source_type": "csv", "path": "data/municipios.csv"},
        "uf_source": {"source_type": "csv", "path": "data/uf.csv"},
        "queimadas_source": {"source_type": "parquet", "path": "data/queimadas.parquet"},
        "sisam_source": {"source_type": "bigquery", "table": "project.dataset.sisam"},
        "dim_data_sink": {"sink_type": "local_parquet", "path": "output/dim_data"},
        "dim_horarios_sink": {"sink_type": "local_parquet", "path": "output/dim_horarios"},
        "dim_local_sink": {"sink_type": "local_parquet", "path": "output/dim_local"},
        "fct_queimadas_sink": {"sink_type": "postgres", "jdbc_url": "jdbc:postgresql://localhost:5432/db", "table_name": "fct_queimadas"},
        "fct_clima_sink": {"sink_type": "gcs_parquet", "path": "gs://bucket/fct_clima"}
    }
    
    config = ETLConfig.from_dict(config_dict)
    
    assert config.spark_config["spark.app.name"] == "Test"
    assert config.municipios_source.source_type == SourceType.CSV
    assert config.sisam_source.source_type == SourceType.BIGQUERY
    assert config.fct_queimadas_sink.sink_type == SinkType.POSTGRES
    assert config.fct_clima_sink.sink_type == SinkType.GCS_PARQUET
