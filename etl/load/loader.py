from pyspark.sql import DataFrame, SparkSession
from etl.config import SinkConfig, SinkType
import os
import logging
import pkgutil

logger = logging.getLogger(__name__)

class Loader:
    def __init__(self, spark: SparkSession, columnar_migration_enabled: bool = False):
        self.spark = spark
        self.columnar_migration_enabled = columnar_migration_enabled
        self.indices = {
            "dim_data": {
                "idx_dim_data_date": "CREATE INDEX idx_dim_data_date ON dim_data(date_time_iso)",
                "idx_dim_data_ano_mes": "CREATE INDEX idx_dim_data_ano_mes ON dim_data(ano, mes)"
            },
            "dim_local_clima": {
                "idx_dim_local_clima_municipio": "CREATE INDEX idx_dim_local_clima_municipio ON dim_local_clima(id_municipio)",
                "idx_dim_local_clima_uf": "CREATE INDEX idx_dim_local_clima_uf ON dim_local_clima(sigla_uf)"
            },
            "dim_local_queimada": {
                "idx_dim_local_queimada_municipio": "CREATE INDEX idx_dim_local_queimada_municipio ON dim_local_queimada(id_municipio)",
                "idx_dim_local_queimada_uf": "CREATE INDEX idx_dim_local_queimada_uf ON dim_local_queimada(sigla_uf)",
                "idx_dim_local_queimada_local_clima": "CREATE INDEX idx_dim_local_queimada_local_clima ON dim_local_queimada(id_local_clima)"
            },
            "dim_horario_clima": {
                "idx_dim_horario_clima_hora": "CREATE INDEX idx_dim_horario_clima_hora ON dim_horario_clima(hora)"
            },
            "dim_horario_queimada": {
                "idx_dim_horario_queimada_hora": "CREATE INDEX idx_dim_horario_queimada_hora ON dim_horario_queimada(hora)",
                "idx_dim_horario_queimada_horario_clima": "CREATE INDEX idx_dim_horario_queimada_horario_clima ON dim_horario_queimada(id_horario_clima)"
            },
            "fct_queimada": {
                "idx_fct_queimada_data": "CREATE INDEX idx_fct_queimada_data ON fct_queimada(id_data)",
                "idx_fct_queimada_local": "CREATE INDEX idx_fct_queimada_local ON fct_queimada(id_local)",
                "idx_fct_queimada_horario": "CREATE INDEX idx_fct_queimada_horario ON fct_queimada(id_horario)"
            },
            "fct_clima": {
                "idx_fct_clima_data": "CREATE INDEX idx_fct_clima_data ON fct_clima(id_data)",
                "idx_fct_clima_local": "CREATE INDEX idx_fct_clima_local ON fct_clima(id_local)",
                "idx_fct_clima_horario": "CREATE INDEX idx_fct_clima_horario ON fct_clima(id_horario)"
            }
        }

    def _is_columnar_migration_enabled(self) -> bool:
        """Check Spark config to decide whether to run columnar migration logic.

        Controlled via the Spark config key
        ``wildfire.columnar.migration.enabled`` and defaults to False when
        missing or malformed.
        """
        return self.columnar_migration_enabled

    def _get_connection(self, config: SinkConfig):
        # Load the PostgreSQL driver explicitly
        try:
            ccl = self.spark.sparkContext._gateway.jvm.java.lang.Thread.currentThread().getContextClassLoader()
            driver_class = ccl.loadClass("org.postgresql.Driver")
            driver = driver_class.newInstance()
            
            props = self.spark.sparkContext._gateway.jvm.java.util.Properties()
            if config.connection_properties:
                for k, v in config.connection_properties.items():
                    props.setProperty(k, v)
            
            return driver.connect(config.jdbc_url, props)
        except Exception as e:
            logger.warning(f"Could not load PostgreSQL driver or connect directly: {e}")
            
        # Try to register the driver class explicitly before using DriverManager
        try:
            self.spark.sparkContext._gateway.jvm.java.lang.Class.forName("org.postgresql.Driver")
        except Exception as e:
            logger.warning(f"Could not register PostgreSQL driver class: {e}")

        driver_manager = self.spark.sparkContext._gateway.jvm.java.sql.DriverManager
        props = self.spark.sparkContext._gateway.jvm.java.util.Properties()
        if config.connection_properties:
            for k, v in config.connection_properties.items():
                props.setProperty(k, v)
        return driver_manager.getConnection(config.jdbc_url, props)

    def _execute_sql(self, config: SinkConfig, sql: str):
        conn = self._get_connection(config)
        try:
            stmt = conn.createStatement()
            # Handle multiple statements if necessary, but JDBC execute usually takes one.
            # We'll assume simple statements or split them if needed.
            stmt.execute(sql)
            stmt.close()
        finally:
            conn.close()

    def run_migrations(self, config: SinkConfig):
        if config.sink_type != SinkType.POSTGRES:
            return

        try:
            # Read migration file from package resources
            if self._is_columnar_migration_enabled():
                sql_bytes = pkgutil.get_data("etl", "migrations/001_init_schema_columnar.sql")
            else:
                sql_bytes = pkgutil.get_data("etl", "migrations/001_init_schema.sql")

            if not sql_bytes:
                raise FileNotFoundError("Migration file not found in package: etl/migrations/001_init_schema.sql")
            
            sql_content = sql_bytes.decode('utf-8')
                
            # Split by semicolon to handle multiple statements
            statements = [s.strip() for s in sql_content.split(';') if s.strip()]
            
            conn = self._get_connection(config)
            try:
                stmt = conn.createStatement()
                for sql in statements:
                    stmt.execute(sql)
                stmt.close()
                logger.info("Migrations executed successfully.")
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"Error running migrations: {e}")
            raise

    def _truncate_table(self, config: SinkConfig):
        try:
            self._execute_sql(config, f"TRUNCATE TABLE {config.table_name} CASCADE")
            logger.info(f"Table {config.table_name} truncated.")
        except Exception as e:
            logger.warning(f"Could not truncate table {config.table_name}: {e}")

    def _drop_indices(self, config: SinkConfig):
        if config.table_name not in self.indices:
            return
            
        for index_name in self.indices[config.table_name]:
            try:
                self._execute_sql(config, f"DROP INDEX IF EXISTS {index_name}")
                logger.info(f"Dropped index {index_name}")
            except Exception as e:
                logger.warning(f"Failed to drop index {index_name}: {e}")

    def _create_indices(self, config: SinkConfig):
        if config.table_name not in self.indices:
            return
            
        for index_name, create_sql in self.indices[config.table_name].items():
            try:
                # Ensure the SQL uses the correct table name if it was dynamic (though here we use the key)
                # The create_sql in the dict has hardcoded table names. 
                # If config.table_name matches the key, it should be fine.
                # But if table names are dynamic (e.g. schema prefix), we might need to adjust.
                # For now assuming table names match keys.
                self._execute_sql(config, create_sql)
                logger.info(f"Created index {index_name}")
            except Exception as e:
                logger.warning(f"Failed to create index {index_name}: {e}")

    def _disable_constraints(self, config: SinkConfig):
        try:
            self._execute_sql(config, f"ALTER TABLE {config.table_name} DISABLE TRIGGER ALL")
            logger.info(f"Disabled constraints for {config.table_name}")
        except Exception as e:
            logger.warning(f"Failed to disable constraints for {config.table_name}: {e}")

    def _enable_constraints(self, config: SinkConfig):
        try:
            self._execute_sql(config, f"ALTER TABLE {config.table_name} ENABLE TRIGGER ALL")
            logger.info(f"Enabled constraints for {config.table_name}")
        except Exception as e:
            logger.warning(f"Failed to enable constraints for {config.table_name}: {e}")

    def load(self, df: DataFrame, config: SinkConfig):
        if config.sink_type in [SinkType.LOCAL_PARQUET, SinkType.GCS_PARQUET]:
            writer = df.write.mode(config.mode)
            writer.parquet(config.path)
            
        elif config.sink_type == SinkType.POSTGRES:
            columnar_enabled = self._is_columnar_migration_enabled()
            logger.info(f"Columnar migration enabled: {columnar_enabled}")

            # Run migrations (idempotent)
            self.run_migrations(config)
            
            mode = config.mode
            if mode == "overwrite":
                self._truncate_table(config)
                mode = "append"

            # Drop indices before loading to speed up insertion
            self._drop_indices(config)

            # Disable constraints
            self._disable_constraints(config)

            writer = df.write.mode(mode).format("jdbc")
            writer = writer.option("url", config.jdbc_url)
            writer = writer.option("dbtable", config.table_name)
            writer = writer.option("driver", "org.postgresql.Driver")
            writer = writer.option("batchsize", "50000")
            
            # Ensure rewriteBatchedStatements is true for performance
            props = config.connection_properties.copy() if config.connection_properties else {}
            if "rewriteBatchedStatements" not in props:
                props["rewriteBatchedStatements"] = "true"
            
            if props:
                for k, v in props.items():
                    writer = writer.option(k, v)
            writer.save()
            
            # Enable constraints
            self._enable_constraints(config)

            # Recreate indices after loading
            self._create_indices(config)
            
        else:
            raise ValueError(f"Unsupported sink type: {config.sink_type}")
