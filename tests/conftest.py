"""Pytest configuration and shared fixtures."""
import pytest
import polars as pl
import os
from pathlib import Path

PARQUET_SUFFIXES = (".parquet", ".pqt", ".pqt.zstd")
PARQUET_DIR_GLOBS = ("*.parquet", "*.pqt", "*.pqt.zstd")
PARQUET_NAME_ALIASES = {
    "dim_local": ["dim_local_queimada"],
    "dim_horarios": ["dim_horarios_queimada"],
}


def _candidate_names(table_name: str):
    yield table_name
    for alias in PARQUET_NAME_ALIASES.get(table_name, []):
        yield alias


def _collect_parquet_sources(dist_path: str | None, table_name: str):
    if not dist_path:
        return []
    base_path = Path(dist_path)
    found = []
    seen = set()

    for candidate in _candidate_names(table_name):
        dir_path = base_path / candidate
        if dir_path.is_dir():
            for pattern in PARQUET_DIR_GLOBS:
                for path in dir_path.glob(pattern):
                    resolved = path.resolve()
                    if resolved not in seen:
                        seen.add(resolved)
                        found.append(str(resolved))
            continue

        for suffix in PARQUET_SUFFIXES:
            path = base_path / f"{candidate}{suffix}"
            if path.exists():
                resolved = path.resolve()
                if resolved not in seen:
                    seen.add(resolved)
                    found.append(str(resolved))

    return found

def pytest_addoption(parser):
    parser.addoption(
        "--source", 
        action="store", 
        default="parquet", 
        help="Data source: 'parquet' or 'postgres'"
    )
    parser.addoption(
        "--dist-dir", 
        action="store", 
        default="dist", 
        help="Path to dist directory for parquet source"
    )
    parser.addoption(
        "--draft-dist-dir",
        action="store",
        default=None,
        help="Optional path to draft dist directory for parquet source"
    )
    parser.addoption(
        "--postgres-uri", 
        action="store", 
        default="postgresql://postgres:postgres@localhost:5432/olap", 
        help="Postgres URI"
    )

@pytest.fixture(scope="session")
def data_source(request):
    return request.config.getoption("--source")

@pytest.fixture(scope="session")
def dist_dir(request):
    return request.config.getoption("--dist-dir")

@pytest.fixture(scope="session")
def draft_dist_dir(request):
    return request.config.getoption("--draft-dist-dir")

@pytest.fixture(scope="session")
def postgres_uri(request):
    return request.config.getoption("--postgres-uri")

def load_table(source, name, dist_path, pg_uri, draft_path=None, lazy=False):
    """Load a table from the specified source.
    
    Args:
        source: Data source type ('parquet' or 'postgres')
        name: Table name to load
        dist_path: Path to distribution directory
        pg_uri: PostgreSQL connection URI
        draft_path: Optional path to draft directory (checked first)
        lazy: If True, return LazyFrame for parquet sources (memory efficient)
    
    Returns:
        DataFrame or LazyFrame depending on lazy parameter
    """
    if source == "parquet":
        search_roots = []
        if draft_path:
            search_roots.append(draft_path)
        search_roots.append(dist_path)

        for root in search_roots:
            paths = _collect_parquet_sources(root, name)
            if not paths:
                continue
            try:
                if lazy:
                    return pl.scan_parquet(paths)
                else:
                    return pl.read_parquet(paths)
            except Exception as e:
                print(f"Warning: Failed to load {name} from {paths}: {e}")
        print(
            f"Warning: Could not find parquet output for {name} under {[r for r in search_roots if r]}"
        )
        return pl.LazyFrame() if lazy else pl.DataFrame()
            
    elif source == "postgres":
        try:
            # Postgres doesn't support lazy loading in the same way
            return pl.read_database_uri(f"SELECT * FROM {name}", pg_uri)
        except Exception as e:
            print(f"Warning: Could not load {name} from Postgres: {e}")
            return pl.DataFrame()
    else:
        raise ValueError(f"Unknown source: {source}")

@pytest.fixture(scope="session")
def dim_data(data_source, dist_dir, draft_dist_dir, postgres_uri):
    return load_table(data_source, "dim_data", dist_dir, postgres_uri, draft_dist_dir)

@pytest.fixture(scope="session")
def dim_local_queimada(data_source, dist_dir, draft_dist_dir, postgres_uri):
    # Maps to 'dim_local' in the new ETL structure
    return load_table(data_source, "dim_local", dist_dir, postgres_uri, draft_dist_dir)

@pytest.fixture(scope="session")
def dim_local_clima(data_source, dist_dir, draft_dist_dir, postgres_uri):
    return load_table(data_source, "dim_local_clima", dist_dir, postgres_uri, draft_dist_dir)

@pytest.fixture(scope="session")
def dim_horarios_queimada(data_source, dist_dir, draft_dist_dir, postgres_uri):
    # Maps to 'dim_horarios' in the new ETL structure
    return load_table(data_source, "dim_horarios", dist_dir, postgres_uri, draft_dist_dir)

@pytest.fixture(scope="session")
def dim_horarios_clima(data_source, dist_dir, draft_dist_dir, postgres_uri):
    # Maps to 'dim_horarios' in the new ETL structure
    return load_table(data_source, "dim_horarios", dist_dir, postgres_uri, draft_dist_dir)

@pytest.fixture(scope="session")
def fct_clima_lazy(data_source, dist_dir, draft_dist_dir, postgres_uri):
    """Lazy-loaded version of fct_clima for memory-efficient testing."""
    if data_source == "parquet":
        return load_table(data_source, "fct_clima", dist_dir, postgres_uri, draft_dist_dir, lazy=True)
    else:
        # For postgres, return regular version (can't lazy load)
        return load_table(data_source, "fct_clima", dist_dir, postgres_uri, draft_dist_dir)

@pytest.fixture(scope="session")
def fct_queimadas_lazy(data_source, dist_dir, draft_dist_dir, postgres_uri):
    """Lazy-loaded version of fct_queimadas for memory-efficient testing."""
    if data_source == "parquet":
        return load_table(data_source, "fct_queimadas", dist_dir, postgres_uri, draft_dist_dir, lazy=True)
    else:
        # For postgres, return regular version (can't lazy load)
        return load_table(data_source, "fct_queimadas", dist_dir, postgres_uri, draft_dist_dir)

@pytest.fixture(scope="session")
def fct_clima_sample(fct_clima_lazy):
    """Small sample of fct_clima for quick validation tests."""
    if isinstance(fct_clima_lazy, pl.LazyFrame):
        # Take first 10000 rows for validation
        return fct_clima_lazy.head(10000).collect()
    else:
        return fct_clima_lazy.head(10000)
