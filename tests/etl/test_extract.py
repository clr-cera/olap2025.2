import pytest
from unittest.mock import MagicMock
from etl.extract import Extractor
from etl.config import SourceConfig, SourceType

def test_extract_csv():
    # Mock spark session
    mock_spark = MagicMock()
    mock_read = MagicMock()
    mock_spark.read = mock_read
    
    # Setup chain calls
    mock_read.format.return_value = mock_read
    mock_read.option.return_value = mock_read
    mock_read.load.return_value = "dataframe"
    
    extractor = Extractor(mock_spark)
    config = SourceConfig(SourceType.CSV, path="path/to/file.csv", options={"sep": ";"})
    
    df = extractor.extract(config)
    
    assert df == "dataframe"
    mock_read.format.assert_called_with("csv")
    mock_read.load.assert_called_with("path/to/file.csv")

def test_extract_parquet():
    mock_spark = MagicMock()
    mock_read = MagicMock()
    mock_spark.read = mock_read
    mock_read.parquet.return_value = "dataframe"
    
    extractor = Extractor(mock_spark)
    config = SourceConfig(SourceType.PARQUET, path="path/to/file.parquet")
    
    df = extractor.extract(config)
    
    assert df == "dataframe"
    mock_read.parquet.assert_called_with("path/to/file.parquet")

def test_extract_bigquery():
    mock_spark = MagicMock()
    mock_read = MagicMock()
    mock_spark.read = mock_read
    
    mock_read.format.return_value = mock_read
    mock_read.option.return_value = mock_read
    mock_read.load.return_value = "dataframe"
    
    extractor = Extractor(mock_spark)
    config = SourceConfig(SourceType.BIGQUERY, table="project.dataset.table")
    
    df = extractor.extract(config)
    
    assert df == "dataframe"
    mock_read.format.assert_called_with("bigquery")
    mock_read.option.assert_called_with("table", "project.dataset.table")
