"""Schema validation tests for fct_queimadas."""
import polars as pl


class TestFctQueimadasSchema:
    """Test suite for fct_queimadas schema validation."""

    def test_columns_exist(self, fct_queimadas):
        """Test that all expected columns exist."""
        expected_columns = {
            "id_data", "id_local", "id_horario",
            "precipitacao", "risco_fogo", "potencia_radiativa_fogo",
            "dias_sem_chuva"
        }
        actual_columns = set(fct_queimadas.columns)
        assert expected_columns == actual_columns, (
            f"Column mismatch. Expected: {expected_columns}, Got: {actual_columns}"
        )

    def test_id_data_dtype(self, fct_queimadas):
        """Test that id_data has correct data type."""
        assert fct_queimadas["id_data"].dtype == pl.UInt32

    def test_id_local_dtype(self, fct_queimadas):
        """Test that id_local has correct data type."""
        assert fct_queimadas["id_local"].dtype == pl.UInt32

    def test_id_horario_dtype(self, fct_queimadas):
        """Test that id_horario has correct data type."""
        assert fct_queimadas["id_horario"].dtype == pl.Int32

    def test_precipitacao_dtype(self, fct_queimadas):
        """Test that precipitacao has float type."""
        assert fct_queimadas["precipitacao"].dtype == pl.Float64

    def test_risco_fogo_dtype(self, fct_queimadas):
        """Test that risco_fogo has float type."""
        assert fct_queimadas["risco_fogo"].dtype == pl.Float64

    def test_potencia_radiativa_fogo_dtype(self, fct_queimadas):
        """Test that potencia_radiativa_fogo has float type."""
        assert fct_queimadas["potencia_radiativa_fogo"].dtype == pl.Float64

    def test_dias_sem_chuva_dtype(self, fct_queimadas):
        """Test that dias_sem_chuva has integer type."""
        assert fct_queimadas["dias_sem_chuva"].dtype == pl.Int64

    def test_no_nulls_id_data(self, fct_queimadas):
        """Test that id_data has no null values."""
        assert fct_queimadas["id_data"].null_count() == 0

    def test_no_nulls_id_local(self, fct_queimadas):
        """Test that id_local has no null values."""
        assert fct_queimadas["id_local"].null_count() == 0

    def test_no_nulls_id_horario(self, fct_queimadas):
        """Test that id_horario has no null values."""
        assert fct_queimadas["id_horario"].null_count() == 0

    def test_precipitacao_non_negative(self, fct_queimadas):
        """Test that precipitacao values are non-negative where not null."""
        non_null_precipitacao = fct_queimadas.filter(
            pl.col("precipitacao").is_not_null()
        )
        if len(non_null_precipitacao) > 0:
            assert non_null_precipitacao["precipitacao"].min() >= 0

    def test_risco_fogo_non_negative(self, fct_queimadas):
        """Test that risco_fogo values are non-negative where not null."""
        non_null_risco = fct_queimadas.filter(
            pl.col("risco_fogo").is_not_null()
        )
        if len(non_null_risco) > 0:
            assert non_null_risco["risco_fogo"].min() >= 0

    def test_dias_sem_chuva_non_negative(self, fct_queimadas):
        """Test that dias_sem_chuva values are non-negative where not null."""
        non_null_dias = fct_queimadas.filter(
            pl.col("dias_sem_chuva").is_not_null()
        )
        if len(non_null_dias) > 0:
            assert non_null_dias["dias_sem_chuva"].min() >= 0

    def test_has_records(self, fct_queimadas):
        """Test that the fact table has at least one record."""
        assert len(fct_queimadas) > 0


class TestFctQueimadasReferentialIntegrity:
    """Test suite for referential integrity of fct_queimadas."""

    def test_id_data_references_dim_data(self, fct_queimadas, dim_data):
        """Test that all id_data values exist in dim_data."""
        valid_ids = set(dim_data["id_data"].unique().to_list())
        fact_ids = set(fct_queimadas["id_data"].unique().to_list())
        invalid_ids = fact_ids - valid_ids
        assert len(invalid_ids) == 0, (
            f"Found {len(invalid_ids)} invalid id_data values in fact table"
        )

    def test_id_local_references_dim_local(self, fct_queimadas, dim_local_queimada):
        """Test that all id_local values exist in dim_local_queimada."""
        valid_ids = set(dim_local_queimada["id_local"].unique().to_list())
        fact_ids = set(fct_queimadas["id_local"].unique().to_list())
        invalid_ids = fact_ids - valid_ids
        assert len(invalid_ids) == 0, (
            f"Found {len(invalid_ids)} invalid id_local values in fact table"
        )

    def test_id_horario_references_dim_horarios(self, fct_queimadas, dim_horarios_queimada):
        """Test that all id_horario values exist in dim_horarios_queimada."""
        valid_ids = set(dim_horarios_queimada["id_horario"].unique().to_list())
        fact_ids = set(fct_queimadas["id_horario"].unique().to_list())
        invalid_ids = fact_ids - valid_ids
        assert len(invalid_ids) == 0, (
            f"Found {len(invalid_ids)} invalid id_horario values in fact table"
        )
