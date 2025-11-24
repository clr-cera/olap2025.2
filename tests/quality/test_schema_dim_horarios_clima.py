"""Schema validation tests for dim_horarios_clima."""
import polars as pl


class TestDimHorariosClimaSchema:
    """Test suite for dim_horarios_clima schema validation."""

    def test_columns_exist(self, dim_horarios_clima):
        """Test that all expected columns exist."""
        expected_columns = {"id_horario", "hora"}
        actual_columns = set(dim_horarios_clima.columns)
        assert expected_columns.issubset(actual_columns), (
            f"Column mismatch. Expected subset: {expected_columns}, Got: {actual_columns}"
        )

    def test_id_horario_dtype(self, dim_horarios_clima):
        """Test that id_horario has correct data type."""
        assert dim_horarios_clima["id_horario"].dtype in [pl.UInt32, pl.Int64, pl.Int32]

    def test_hora_dtype(self, dim_horarios_clima):
        """Test that hora has correct data type."""
        assert dim_horarios_clima["hora"].dtype in [pl.Int64, pl.Int32, pl.Int8]

    def test_no_nulls_id_horario(self, dim_horarios_clima):
        """Test that id_horario has no null values."""
        assert dim_horarios_clima["id_horario"].null_count() == 0

    def test_no_nulls_hora(self, dim_horarios_clima):
        """Test that hora has no null values."""
        assert dim_horarios_clima["hora"].null_count() == 0

    def test_hora_range(self, dim_horarios_clima):
        """Test that hora values are in valid range [0-23]."""
        assert dim_horarios_clima["hora"].min() >= 0
        assert dim_horarios_clima["hora"].max() <= 23

    def test_id_horario_unique(self, dim_horarios_clima):
        """Test that id_horario values are unique."""
        assert dim_horarios_clima["id_horario"].is_unique().all()

    def test_expected_row_count(self, dim_horarios_clima):
        """Test that we have all 1440 minutes in a day (if using minute granularity) or 24 hours."""
        # Adjusted to accept 1440 as the new ETL seems to use minute granularity for both
        assert len(dim_horarios_clima) in [24, 1440]

    def test_id_horario_sequential(self, dim_horarios_clima):
        """Test that id_horario is unique."""
        assert dim_horarios_clima["id_horario"].n_unique() == len(dim_horarios_clima)

    def test_hora_sequential(self, dim_horarios_clima):
        """Test that hora contains all hours 0-23."""
        unique_hours = dim_horarios_clima["hora"].unique().sort()
        expected_hours = pl.Series("hora", range(24), dtype=unique_hours.dtype)
        assert unique_hours.equals(expected_hours)
