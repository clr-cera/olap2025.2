"""Schema validation tests for fct_clima."""
import polars as pl


class TestFctClimaSchema:
    """Test suite for fct_clima schema validation."""

    def test_columns_exist(self, fct_clima):
        """Test that all expected columns exist."""
        expected_columns = {
            "id_data", "id_local", "id_horario", "temperatura", "umidade_relativa",
            "vento_velocidade", "vento_direcao", "co_ppb", "no2_ppb",
            "o3_ppb", "pm25_ugm3", "so2_ugm3", "precipitacao_dia"
        }
        actual_columns = set(fct_clima.columns)
        assert expected_columns == actual_columns, (
            f"Column mismatch. Expected: {expected_columns}, Got: {actual_columns}"
        )

    def test_id_data_dtype(self, fct_clima):
        """Test that id_data has correct data type."""
        assert fct_clima["id_data"].dtype == pl.UInt32

    def test_id_local_dtype(self, fct_clima):
        """Test that id_local has correct data type."""
        assert fct_clima["id_local"].dtype == pl.UInt32

    def test_temperatura_dtype(self, fct_clima):
        """Test that temperatura has float type."""
        assert fct_clima["temperatura"].dtype == pl.Float64

    def test_umidade_relativa_dtype(self, fct_clima):
        """Test that umidade_relativa has float type."""
        assert fct_clima["umidade_relativa"].dtype == pl.Float64

    def test_vento_velocidade_dtype(self, fct_clima):
        """Test that vento_velocidade has float type."""
        assert fct_clima["vento_velocidade"].dtype == pl.Float64

    def test_vento_direcao_dtype(self, fct_clima):
        """Test that vento_direcao has numeric type (Int64 or Float64)."""
        assert fct_clima["vento_direcao"].dtype in [pl.Float64, pl.Int64]

    def test_co_ppb_dtype(self, fct_clima):
        """Test that co_ppb has float type."""
        assert fct_clima["co_ppb"].dtype == pl.Float64

    def test_no2_ppb_dtype(self, fct_clima):
        """Test that no2_ppb has float type."""
        assert fct_clima["no2_ppb"].dtype == pl.Float64

    def test_o3_ppb_dtype(self, fct_clima):
        """Test that o3_ppb has float type."""
        assert fct_clima["o3_ppb"].dtype == pl.Float64

    def test_pm25_ugm3_dtype(self, fct_clima):
        """Test that pm25_ugm3 has float type."""
        assert fct_clima["pm25_ugm3"].dtype == pl.Float64

    def test_so2_ugm3_dtype(self, fct_clima):
        """Test that so2_ugm3 has float type."""
        assert fct_clima["so2_ugm3"].dtype == pl.Float64

    def test_precipitacao_dia_dtype(self, fct_clima):
        """Test that precipitacao_dia has float type."""
        assert fct_clima["precipitacao_dia"].dtype == pl.Float64

    def test_no_nulls_id_data(self, fct_clima):
        """Test that id_data has no null values."""
        assert fct_clima["id_data"].null_count() == 0

    def test_no_nulls_id_local(self, fct_clima):
        """Test that id_local has no null values."""
        assert fct_clima["id_local"].null_count() == 0

    def test_temperatura_reasonable_range(self, fct_clima):
        """Test that temperatura values are in reasonable range [-50, 60] Celsius."""
        non_null_temp = fct_clima.filter(pl.col("temperatura").is_not_null())
        if len(non_null_temp) > 0:
            assert non_null_temp["temperatura"].min() >= -50
            assert non_null_temp["temperatura"].max() <= 60

    def test_umidade_relativa_range(self, fct_clima):
        """Test that umidade_relativa values are in valid range [0, 100] percent."""
        non_null_umidade = fct_clima.filter(pl.col("umidade_relativa").is_not_null())
        if len(non_null_umidade) > 0:
            assert non_null_umidade["umidade_relativa"].min() >= 0
            assert non_null_umidade["umidade_relativa"].max() <= 100

    def test_vento_velocidade_non_negative(self, fct_clima):
        """Test that vento_velocidade values are non-negative where not null."""
        non_null_vento = fct_clima.filter(pl.col("vento_velocidade").is_not_null())
        if len(non_null_vento) > 0:
            assert non_null_vento["vento_velocidade"].min() >= 0

    def test_vento_direcao_range(self, fct_clima):
        """Test that vento_direcao values are in valid range [0, 360] degrees."""
        non_null_direcao = fct_clima.filter(pl.col("vento_direcao").is_not_null())
        if len(non_null_direcao) > 0:
            assert non_null_direcao["vento_direcao"].min() >= 0
            assert non_null_direcao["vento_direcao"].max() <= 360

    def test_air_quality_non_negative(self, fct_clima):
        """Test that all air quality measures are non-negative where not null."""
        aq_columns = ["co_ppb", "no2_ppb", "o3_ppb", "pm25_ugm3", "so2_ugm3"]
        for col in aq_columns:
            non_null_data = fct_clima.filter(pl.col(col).is_not_null())
            if len(non_null_data) > 0:
                assert non_null_data[col].min() >= 0, f"{col} has negative values"

    def test_precipitacao_dia_non_negative(self, fct_clima):
        """Test that precipitacao_dia values are non-negative where not null."""
        non_null_precip = fct_clima.filter(pl.col("precipitacao_dia").is_not_null())
        if len(non_null_precip) > 0:
            assert non_null_precip["precipitacao_dia"].min() >= 0

    def test_has_records(self, fct_clima):
        """Test that the fact table has at least one record."""
        assert len(fct_clima) > 0


class TestFctClimaReferentialIntegrity:
    """Test suite for referential integrity of fct_clima."""

    def test_id_data_references_dim_data(self, fct_clima, dim_data):
        """Test that all id_data values exist in dim_data."""
        valid_ids = set(dim_data["id_data"].unique().to_list())
        fact_ids = set(fct_clima["id_data"].unique().to_list())
        invalid_ids = fact_ids - valid_ids
        assert len(invalid_ids) == 0, (
            f"Found {len(invalid_ids)} invalid id_data values in fact table"
        )

    def test_id_local_references_dim_local_clima(self, fct_clima, dim_local_clima):
        """Test that all id_local values exist in dim_local_clima."""
        valid_ids = set(dim_local_clima["id_local"].unique().to_list())
        fact_ids = set(fct_clima["id_local"].unique().to_list())
        invalid_ids = fact_ids - valid_ids
        assert len(invalid_ids) == 0, (
            f"Found {len(invalid_ids)} invalid id_local values in fact table"
        )
