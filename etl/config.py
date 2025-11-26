from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, Any

class SourceType(Enum):
    CSV = "csv"
    PARQUET = "parquet"
    BIGQUERY = "bigquery"

class SinkType(Enum):
    LOCAL_PARQUET = "local_parquet"
    GCS_PARQUET = "gcs_parquet"
    POSTGRES = "postgres"

@dataclass
class SourceConfig:
    source_type: SourceType
    path: Optional[str] = None # For CSV/Parquet
    table: Optional[str] = None # For BigQuery
    options: Optional[Dict[str, Any]] = None

@dataclass
class SinkConfig:
    sink_type: SinkType
    path: Optional[str] = None # For Parquet (Local/GCS)
    connection_properties: Optional[Dict[str, Any]] = None # For Postgres
    jdbc_url: Optional[str] = None # For Postgres
    table_name: Optional[str] = None # For Postgres
    mode: str = "overwrite"

@dataclass
class ETLConfig:
    # General
    columnar_migration_enabled: bool
    spark_config: Dict[str, Any]
    
    # Sources
    municipios_source: SourceConfig
    uf_source: SourceConfig
    queimadas_source: SourceConfig
    sisam_source: SourceConfig
    
    # Sinks
    dim_data_sink: SinkConfig
    dim_horarios_sink: SinkConfig
    dim_local_sink: SinkConfig
    fct_queimadas_sink: SinkConfig
    fct_clima_sink: SinkConfig

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> 'ETLConfig':
        def create_source(cfg):
            return SourceConfig(SourceType(cfg['source_type']), cfg.get('path'), cfg.get('table'), cfg.get('options'))
        
        def create_sink(cfg):
            return SinkConfig(SinkType(cfg['sink_type']), cfg.get('path'), cfg.get('connection_properties'), cfg.get('jdbc_url'), cfg.get('table_name'), cfg.get('mode', 'overwrite'))

        return cls(
            columnar_migration_enabled=config.get('columnar_migration_enabled', False),
            spark_config=config.get('spark_config', {}),
            municipios_source=create_source(config['municipios_source']),
            uf_source=create_source(config['uf_source']),
            queimadas_source=create_source(config['queimadas_source']),
            sisam_source=create_source(config['sisam_source']),
            dim_data_sink=create_sink(config['dim_data_sink']),
            dim_horarios_sink=create_sink(config['dim_horarios_sink']),
            dim_local_sink=create_sink(config['dim_local_sink']),
            fct_queimadas_sink=create_sink(config['fct_queimadas_sink']),
            fct_clima_sink=create_sink(config['fct_clima_sink'])
        )
