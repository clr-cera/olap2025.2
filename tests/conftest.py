"""Pytest configuration and shared fixtures."""
import pytest
import polars as pl
from pathlib import Path


@pytest.fixture(scope="session")
def data_dir():
    """Return the data directory path."""
    return Path(__file__).parent.parent / "data"


@pytest.fixture(scope="session")
def dim_horarios_queimada(data_dir):
    """Load dim_horarios_queimada data."""
    return pl.read_parquet(data_dir / "dim_horarios_queimada.pqt.zstd")


@pytest.fixture(scope="session")
def dim_local_queimada(data_dir):
    """Load dim_local_queimada data."""
    return pl.read_parquet(data_dir / "dim_local_queimada.pqt.zstd")


@pytest.fixture(scope="session")
def dim_data(data_dir):
    """Load dim_data data."""
    return pl.read_parquet(data_dir / "dim_data.pqt.zstd")


@pytest.fixture(scope="session")
def fct_queimadas(data_dir):
    """Load fct_queimadas data."""
    return pl.read_parquet(data_dir / "fct_queimadas.pqt.zstd")


@pytest.fixture(scope="session")
def dim_horarios_clima(data_dir):
    """Load dim_horarios_clima data."""
    return pl.read_parquet(data_dir / "dim_horarios_clima.pqt.zstd")


@pytest.fixture(scope="session")
def dim_local_clima(data_dir):
    """Load dim_local_clima data."""
    return pl.read_parquet(data_dir / "dim_local_clima.pqt.zstd")


@pytest.fixture(scope="session")
def fct_clima(data_dir):
    """Load fct_clima data."""
    return pl.read_parquet(data_dir / "fct_clima.pqt.zstd")
