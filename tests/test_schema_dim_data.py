"""Schema validation tests for dim_data."""
import polars as pl


class TestDimDataSchema:
    """Test suite for dim_data schema validation."""

    def test_columns_exist(self, dim_data):
        """Test that all expected columns exist."""
        expected_columns = {
            "id_data", "date_time_iso", "dia", "mes", "ano",
            "semestre", "trimestre", "dia_semana", "dia_ano",
            "is_weekend", "semana_ano", "estacao"
        }
        actual_columns = set(dim_data.columns)
        assert expected_columns == actual_columns, (
            f"Column mismatch. Expected: {expected_columns}, Got: {actual_columns}"
        )

    def test_id_data_dtype(self, dim_data):
        """Test that id_data has correct data type."""
        assert dim_data["id_data"].dtype == pl.UInt32

    def test_date_time_iso_dtype(self, dim_data):
        """Test that date_time_iso has datetime type."""
        assert dim_data["date_time_iso"].dtype in [pl.Datetime, pl.Datetime("us"), pl.Datetime("ns"), pl.Datetime("ms")]

    def test_dia_dtype(self, dim_data):
        """Test that dia has correct data type."""
        assert dim_data["dia"].dtype == pl.Int8

    def test_mes_dtype(self, dim_data):
        """Test that mes has correct data type."""
        assert dim_data["mes"].dtype == pl.Int8

    def test_ano_dtype(self, dim_data):
        """Test that ano has correct data type."""
        assert dim_data["ano"].dtype == pl.Int32

    def test_semestre_dtype(self, dim_data):
        """Test that semestre has correct data type."""
        assert dim_data["semestre"].dtype == pl.Int64

    def test_trimestre_dtype(self, dim_data):
        """Test that trimestre has correct data type."""
        assert dim_data["trimestre"].dtype == pl.Int8

    def test_dia_semana_dtype(self, dim_data):
        """Test that dia_semana has correct data type."""
        assert dim_data["dia_semana"].dtype == pl.Int8

    def test_dia_ano_dtype(self, dim_data):
        """Test that dia_ano has correct data type."""
        assert dim_data["dia_ano"].dtype == pl.Int16

    def test_is_weekend_dtype(self, dim_data):
        """Test that is_weekend has boolean type."""
        assert dim_data["is_weekend"].dtype == pl.Boolean

    def test_semana_ano_dtype(self, dim_data):
        """Test that semana_ano has correct data type."""
        assert dim_data["semana_ano"].dtype == pl.Int8

    def test_estacao_dtype(self, dim_data):
        """Test that estacao has correct data type."""
        assert dim_data["estacao"].dtype == pl.Int32

    def test_no_nulls_id_data(self, dim_data):
        """Test that id_data has no null values."""
        assert dim_data["id_data"].null_count() == 0

    def test_no_nulls_date_time_iso(self, dim_data):
        """Test that date_time_iso has no null values."""
        assert dim_data["date_time_iso"].null_count() == 0

    def test_id_data_unique(self, dim_data):
        """Test that id_data values are unique."""
        assert dim_data["id_data"].is_unique().all()

    def test_date_time_iso_unique(self, dim_data):
        """Test that date_time_iso values are unique."""
        assert dim_data["date_time_iso"].is_unique().all()

    def test_dia_range(self, dim_data):
        """Test that dia values are in valid range [1-31]."""
        assert dim_data["dia"].min() >= 1
        assert dim_data["dia"].max() <= 31

    def test_mes_range(self, dim_data):
        """Test that mes values are in valid range [1-12]."""
        assert dim_data["mes"].min() >= 1
        assert dim_data["mes"].max() <= 12

    def test_semestre_range(self, dim_data):
        """Test that semestre values are in valid range [1-2]."""
        assert dim_data["semestre"].min() >= 1
        assert dim_data["semestre"].max() <= 2

    def test_trimestre_range(self, dim_data):
        """Test that trimestre values are in valid range [1-4]."""
        assert dim_data["trimestre"].min() >= 1
        assert dim_data["trimestre"].max() <= 4

    def test_dia_semana_range(self, dim_data):
        """Test that dia_semana values are in valid range [1-7]."""
        assert dim_data["dia_semana"].min() >= 1
        assert dim_data["dia_semana"].max() <= 7

    def test_dia_ano_range(self, dim_data):
        """Test that dia_ano values are in valid range [1-366]."""
        assert dim_data["dia_ano"].min() >= 1
        assert dim_data["dia_ano"].max() <= 366

    def test_semana_ano_range(self, dim_data):
        """Test that semana_ano values are in valid range [1-53]."""
        assert dim_data["semana_ano"].min() >= 1
        assert dim_data["semana_ano"].max() <= 53

    def test_estacao_range(self, dim_data):
        """Test that estacao values are in valid range [1-4]."""
        assert dim_data["estacao"].min() >= 1
        assert dim_data["estacao"].max() <= 4

    def test_is_weekend_logic(self, dim_data):
        """Test that is_weekend is True for Saturday (6) and Sunday (7)."""
        weekend_df = dim_data.filter(pl.col("dia_semana") >= 6)
        if len(weekend_df) > 0:
            assert weekend_df["is_weekend"].all()
        
        weekday_df = dim_data.filter(pl.col("dia_semana") < 6)
        if len(weekday_df) > 0:
            assert not weekday_df["is_weekend"].any()

    def test_data_sorted(self, dim_data):
        """Test that data is sorted by id_data (which should correlate with time order)."""
        # Check that id_data is sorted (monotonically increasing)
        sorted_ids = dim_data["id_data"].sort()
        assert dim_data["id_data"].equals(sorted_ids), "id_data should be sorted"
