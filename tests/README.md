# Data Schema Validation Tests

This directory contains comprehensive schema validation tests for the wildfire data exported by the ETL notebook.

## Test Coverage

The test suite validates the following data files:

1. **dim_horarios_queimada** (`test_schema_dim_horarios.py`)

   - Column presence and data types
   - Valid hour (0-23) and minute (0-59) ranges
   - Unique and sequential ID values
   - Expected row count (1440 minutes per day)

2. **dim_data** (`test_schema_dim_data.py`)

   - Column presence and data types for all date/time dimensions
   - Valid ranges for days, months, semesters, trimesters, weeks, seasons
   - Weekend logic validation
   - Data uniqueness and sorting

3. **dim_local_queimada** (`test_schema_dim_local.py`)

   - Column presence and data types for location dimensions
   - Valid latitude/longitude ranges for Brazil
   - Valid Brazilian states, regions, and biomes
   - Data uniqueness and sorting

4. **fct_queimadas** (`test_schema_fct_queimadas.py`)
   - Column presence and data types for fact table
   - Non-negative values for measures
   - **Referential integrity** - validates all foreign keys reference valid dimension records

## Running the Tests

### Run all tests:

```bash
pytest tests/
```

### Run tests with verbose output:

```bash
pytest tests/ -v
```

### Run tests for a specific dimension:

```bash
pytest tests/test_schema_dim_horarios.py -v
pytest tests/test_schema_dim_data.py -v
pytest tests/test_schema_dim_local.py -v
pytest tests/test_schema_fct_queimadas.py -v
```

### Run a specific test class:

```bash
pytest tests/test_schema_fct_queimadas.py::TestFctQueimadasReferentialIntegrity -v
```

### Run a specific test:

```bash
pytest tests/test_schema_dim_data.py::TestDimDataSchema::test_estacao_range -v
```

### Generate a test report:

```bash
pytest tests/ --html=test-report.html --self-contained-html
```

### Run tests and show coverage:

```bash
pytest tests/ --cov=tests --cov-report=html
```

## Test Structure

- **conftest.py**: Contains pytest fixtures that load the data files once per test session
- **test*schema*\*.py**: Individual test modules for each data file
- Each test class groups related validation tests
- Tests are designed to fail fast and provide clear error messages

## Adding New Tests

To add new validation tests:

1. Add the test method to the appropriate test class
2. Follow the naming convention: `test_<what_is_being_tested>`
3. Include a descriptive docstring
4. Use appropriate fixtures from `conftest.py`
5. Provide clear assertion messages

Example:

```python
def test_new_validation(self, dim_data):
    """Test description here."""
    assert condition, "Clear error message if test fails"
```

## Prerequisites

Make sure your data files exist in the `data/` directory before running tests:

- `dim_horarios_queimada.pqt.zstd`
- `dim_local_queimada.pqt.zstd`
- `dim_data.pqt.zstd`
- `fct_queimadas.pqt.zstd`

Run the ETL notebook (`etl_orig.ipynb`) first to generate these files if they don't exist.
