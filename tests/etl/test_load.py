import pytest
from unittest.mock import MagicMock
from etl.load import Loader
from etl.config import SinkConfig, SinkType

def test_load_parquet(spark):
    mock_df = MagicMock()
    mock_write = MagicMock()
    mock_df.write = mock_write
    mock_write.mode.return_value = mock_write
    
    loader = Loader(spark)
    config = SinkConfig(SinkType.LOCAL_PARQUET, path="output/path")
    
    loader.load(mock_df, config)
    
    mock_write.mode.assert_called_with("overwrite")
    mock_write.parquet.assert_called_with("output/path")

def test_load_postgres(spark):
    mock_df = MagicMock()
    mock_write = MagicMock()
    mock_df.write = mock_write
    mock_write.mode.return_value = mock_write
    mock_write.format.return_value = mock_write
    mock_write.option.return_value = mock_write
    
    loader = Loader(spark)
    # Mock out DB interaction methods to avoid actual connection attempts
    loader.run_migrations = MagicMock()
    loader._truncate_table = MagicMock()
    loader._drop_indices = MagicMock()
    loader._create_indices = MagicMock()
    loader._disable_constraints = MagicMock()
    loader._enable_constraints = MagicMock()

    config = SinkConfig(
        SinkType.POSTGRES, 
        jdbc_url="jdbc:url", 
        table_name="table",
        connection_properties={"user": "u"}
    )
    
    loader.load(mock_df, config)
    
    mock_write.format.assert_called_with("jdbc")
    mock_write.save.assert_called()
