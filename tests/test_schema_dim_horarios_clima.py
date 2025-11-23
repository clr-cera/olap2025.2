"""Schema validation tests for dim_horarios_clima."""
import polars as pl


class TestDimHorariosClimaSchema:
    """Test suite for dim_horarios_clima schema validation."""

    def test_columns_exist(self, dim_horarios_clima):
        """Test that all expected columns exist."""
        expected_columns = {"id_horario", "hora"}
        actual_columns = set(dim_horarios_clima.columns)
        assert expected_columns == actual_columns, (
            f"Column mismatch. Expected: {expected_columns}, Got: {actual_columns}"
        )

    def test_id_horario_dtype(self, dim_horarios_clima):
        """Test that id_horario has correct data type."""
        assert dim_horarios_clima["id_horario"].dtype == pl.UInt32

    def test_hora_dtype(self, dim_horarios_clima):
        """Test that hora has correct data type."""
        assert dim_horarios_clima["hora"].dtype == pl.Int64

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
        """Test that we have all 24 hours in a day."""
        assert len(dim_horarios_clima) == 24

    def test_id_horario_sequential(self, dim_horarios_clima):
        """Test that id_horario is sequential starting from 0."""
        expected_ids = pl.Series("id_horario", range(24), dtype=pl.UInt32)
        assert dim_horarios_clima["id_horario"].equals(expected_ids)

    def test_hora_sequential(self, dim_horarios_clima):
        """Test that hora contains all hours 0-23."""
        expected_horas = pl.Series("hora", range(24), dtype=pl.Int64)
        sorted_horas = dim_horarios_clima.sort("hora")["hora"]
        assert sorted_horas.equals(expected_horas)
