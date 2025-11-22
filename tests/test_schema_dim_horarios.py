"""Schema validation tests for dim_horarios_queimada."""
import polars as pl


class TestDimHorariosQueimadaSchema:
    """Test suite for dim_horarios_queimada schema validation."""

    def test_columns_exist(self, dim_horarios_queimada):
        """Test that all expected columns exist."""
        expected_columns = {"id_horario", "hora", "minuto"}
        actual_columns = set(dim_horarios_queimada.columns)
        assert expected_columns == actual_columns, (
            f"Column mismatch. Expected: {expected_columns}, Got: {actual_columns}"
        )

    def test_id_horario_dtype(self, dim_horarios_queimada):
        """Test that id_horario has correct data type."""
        assert dim_horarios_queimada["id_horario"].dtype == pl.Int32

    def test_hora_dtype(self, dim_horarios_queimada):
        """Test that hora has correct data type."""
        assert dim_horarios_queimada["hora"].dtype == pl.Int8

    def test_minuto_dtype(self, dim_horarios_queimada):
        """Test that minuto has correct data type."""
        assert dim_horarios_queimada["minuto"].dtype == pl.Int8

    def test_no_nulls_id_horario(self, dim_horarios_queimada):
        """Test that id_horario has no null values."""
        assert dim_horarios_queimada["id_horario"].null_count() == 0

    def test_no_nulls_hora(self, dim_horarios_queimada):
        """Test that hora has no null values."""
        assert dim_horarios_queimada["hora"].null_count() == 0

    def test_no_nulls_minuto(self, dim_horarios_queimada):
        """Test that minuto has no null values."""
        assert dim_horarios_queimada["minuto"].null_count() == 0

    def test_hora_range(self, dim_horarios_queimada):
        """Test that hora values are in valid range [0-23]."""
        assert dim_horarios_queimada["hora"].min() >= 0
        assert dim_horarios_queimada["hora"].max() <= 23

    def test_minuto_range(self, dim_horarios_queimada):
        """Test that minuto values are in valid range [0-59]."""
        assert dim_horarios_queimada["minuto"].min() >= 0
        assert dim_horarios_queimada["minuto"].max() <= 59

    def test_id_horario_unique(self, dim_horarios_queimada):
        """Test that id_horario values are unique."""
        assert dim_horarios_queimada["id_horario"].is_unique().all()

    def test_expected_row_count(self, dim_horarios_queimada):
        """Test that we have all 1440 minutes in a day (24 hours * 60 minutes)."""
        assert len(dim_horarios_queimada) == 1440

    def test_id_horario_sequential(self, dim_horarios_queimada):
        """Test that id_horario is sequential starting from 0."""
        expected_ids = pl.Series("id_horario", range(1440), dtype=pl.Int32)
        assert dim_horarios_queimada["id_horario"].equals(expected_ids)
