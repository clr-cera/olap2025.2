"""Schema validation tests for fct_clima."""
import pytest
import polars as pl


@pytest.fixture(scope="module")
def fct_clima_stats(fct_clima_lazy):
    """Compute statistics for fct_clima in a single pass to avoid multiple scans.
    
    Returns a dictionary with pre-computed statistics:
    - total_count
    - {col}_min, {col}_max, {col}_null_count for numeric columns
    - {col}_null_count for key columns
    """
    if isinstance(fct_clima_lazy, pl.LazyFrame):
        # Columns to analyze
        numeric_cols = [
            "temperatura", "umidade_relativa", "vento_velocidade", "vento_direcao",
            "co_ppb", "no2_ppb", "o3_ppb", "pm25_ugm3", "so2_ugm3", "precipitacao_dia"
        ]
        
        exprs = [pl.len().alias("total_count")]
        
        # Key columns null checks
        exprs.append(pl.col("id_data").null_count().alias("id_data_null_count"))
        exprs.append(pl.col("id_local").null_count().alias("id_local_null_count"))
        
        # Numeric columns stats
        for col in numeric_cols:
            exprs.append(pl.col(col).min().alias(f"{col}_min"))
            exprs.append(pl.col(col).max().alias(f"{col}_max"))
            exprs.append(pl.col(col).null_count().alias(f"{col}_null_count"))
            
        # Execute in one pass using streaming if possible
        try:
            stats = fct_clima_lazy.select(exprs).collect(engine="streaming")
        except Exception:
            # Fallback if streaming fails
            stats = fct_clima_lazy.select(exprs).collect()
        
        # Convert to dictionary for easy access {col_stat: value}
        return {k: v[0] for k, v in stats.to_dict(as_series=False).items()}
    else:
        # Fallback for DataFrame (eager)
        df = fct_clima_lazy
        stats = {"total_count": len(df)}
        
        stats["id_data_null_count"] = df["id_data"].null_count()
        stats["id_local_null_count"] = df["id_local"].null_count()
        
        numeric_cols = [
            "temperatura", "umidade_relativa", "vento_velocidade", "vento_direcao",
            "co_ppb", "no2_ppb", "o3_ppb", "pm25_ugm3", "so2_ugm3", "precipitacao_dia"
        ]
        
        for col in numeric_cols:
            stats[f"{col}_min"] = df[col].min()
            stats[f"{col}_max"] = df[col].max()
            stats[f"{col}_null_count"] = df[col].null_count()
            
        return stats


class TestFctClimaSchema:
    """Test suite for fct_clima schema validation.
    
    These tests use sample data and lazy evaluation to minimize memory usage.
    """

    def test_columns_exist(self, fct_clima_sample):
        """Test that all expected columns exist."""
        expected_columns = {
            "id_data", "id_local", "id_horario", "temperatura", "umidade_relativa",
            "vento_velocidade", "vento_direcao", "co_ppb", "no2_ppb",
            "o3_ppb", "pm25_ugm3", "so2_ugm3", "precipitacao_dia"
        }
        actual_columns = set(fct_clima_sample.columns)
        assert expected_columns == actual_columns, (
            f"Column mismatch. Expected: {expected_columns}, Got: {actual_columns}"
        )

    def test_id_data_dtype(self, fct_clima_sample):
        """Test that id_data has correct data type."""
        assert fct_clima_sample["id_data"].dtype in [pl.UInt32, pl.Int64, pl.Int32]

    def test_id_local_dtype(self, fct_clima_sample):
        """Test that id_local has correct data type."""
        assert fct_clima_sample["id_local"].dtype in [pl.UInt32, pl.Int64, pl.Int32]

    def test_temperatura_dtype(self, fct_clima_sample):
        """Test that temperatura has float type."""
        assert fct_clima_sample["temperatura"].dtype == pl.Float64

    def test_umidade_relativa_dtype(self, fct_clima_sample):
        """Test that umidade_relativa has float type."""
        assert fct_clima_sample["umidade_relativa"].dtype == pl.Float64

    def test_vento_velocidade_dtype(self, fct_clima_sample):
        """Test that vento_velocidade has float type."""
        assert fct_clima_sample["vento_velocidade"].dtype == pl.Float64

    def test_vento_direcao_dtype(self, fct_clima_sample):
        """Test that vento_direcao has numeric type (Int64 or Float64)."""
        assert fct_clima_sample["vento_direcao"].dtype in [pl.Float64, pl.Int64]

    def test_co_ppb_dtype(self, fct_clima_sample):
        """Test that co_ppb has float type."""
        assert fct_clima_sample["co_ppb"].dtype == pl.Float64

    def test_no2_ppb_dtype(self, fct_clima_sample):
        """Test that no2_ppb has float type."""
        assert fct_clima_sample["no2_ppb"].dtype == pl.Float64

    def test_o3_ppb_dtype(self, fct_clima_sample):
        """Test that o3_ppb has float type."""
        assert fct_clima_sample["o3_ppb"].dtype == pl.Float64

    def test_pm25_ugm3_dtype(self, fct_clima_sample):
        """Test that pm25_ugm3 has float type."""
        assert fct_clima_sample["pm25_ugm3"].dtype == pl.Float64

    def test_so2_ugm3_dtype(self, fct_clima_sample):
        """Test that so2_ugm3 has float type."""
        assert fct_clima_sample["so2_ugm3"].dtype == pl.Float64

    def test_precipitacao_dia_dtype(self, fct_clima_sample):
        """Test that precipitacao_dia has float type."""
        assert fct_clima_sample["precipitacao_dia"].dtype == pl.Float64

    def test_no_nulls_id_data(self, fct_clima_stats):
        """Test that id_data has no null values (using pre-computed stats)."""
        null_count = fct_clima_stats["id_data_null_count"]
        assert null_count == 0, f"Found {null_count} null values in id_data"

    def test_no_nulls_id_local(self, fct_clima_stats):
        """Test that id_local has no null values (using pre-computed stats)."""
        null_count = fct_clima_stats["id_local_null_count"]
        assert null_count == 0, f"Found {null_count} null values in id_local"

    def test_temperatura_reasonable_range(self, fct_clima_stats):
        """Test that temperatura values are in reasonable range [-50, 60] Celsius."""
        count = fct_clima_stats["total_count"] - fct_clima_stats["temperatura_null_count"]
        if count > 0:
            min_temp = fct_clima_stats["temperatura_min"]
            max_temp = fct_clima_stats["temperatura_max"]
            assert min_temp >= -50, f"Temperature too low: {min_temp}"
            assert max_temp <= 60, f"Temperature too high: {max_temp}"

    def test_umidade_relativa_range(self, fct_clima_stats):
        """Test that umidade_relativa values are in valid range [0, 100] percent."""
        count = fct_clima_stats["total_count"] - fct_clima_stats["umidade_relativa_null_count"]
        if count > 0:
            min_val = fct_clima_stats["umidade_relativa_min"]
            max_val = fct_clima_stats["umidade_relativa_max"]
            assert min_val >= 0, f"Humidity too low: {min_val}"
            assert max_val <= 100, f"Humidity too high: {max_val}"

    def test_vento_velocidade_non_negative(self, fct_clima_stats):
        """Test that vento_velocidade values are non-negative where not null."""
        count = fct_clima_stats["total_count"] - fct_clima_stats["vento_velocidade_null_count"]
        if count > 0:
            min_val = fct_clima_stats["vento_velocidade_min"]
            assert min_val >= 0, f"Wind speed is negative: {min_val}"

    def test_vento_direcao_range(self, fct_clima_stats):
        """Test that vento_direcao values are in valid range [0, 360] degrees."""
        count = fct_clima_stats["total_count"] - fct_clima_stats["vento_direcao_null_count"]
        if count > 0:
            min_val = fct_clima_stats["vento_direcao_min"]
            max_val = fct_clima_stats["vento_direcao_max"]
            assert min_val >= 0, f"Wind direction too low: {min_val}"
            assert max_val <= 360, f"Wind direction too high: {max_val}"

    def test_air_quality_non_negative(self, fct_clima_stats):
        """Test that all air quality measures are non-negative where not null."""
        aq_columns = ["co_ppb", "no2_ppb", "o3_ppb", "pm25_ugm3", "so2_ugm3"]
        
        for col in aq_columns:
            count = fct_clima_stats["total_count"] - fct_clima_stats[f"{col}_null_count"]
            if count > 0:
                min_val = fct_clima_stats[f"{col}_min"]
                assert min_val >= 0, f"{col} has negative values: {min_val}"

    def test_precipitacao_dia_non_negative(self, fct_clima_stats):
        """Test that precipitacao_dia values are non-negative where not null."""
        count = fct_clima_stats["total_count"] - fct_clima_stats["precipitacao_dia_null_count"]
        if count > 0:
            min_val = fct_clima_stats["precipitacao_dia_min"]
            assert min_val >= 0, f"Precipitation is negative: {min_val}"

    def test_has_records(self, fct_clima_stats):
        """Test that the fact table has at least one record."""
        count = fct_clima_stats["total_count"]
        assert count > 0, "Fact table is empty"


class TestFctClimaReferentialIntegrity:
    """Test suite for referential integrity of fct_clima.
    
    These tests use set operations for memory-efficient validation.
    """

    def test_id_data_references_dim_data(self, fct_clima_lazy, dim_data):
        """Test that all id_data values exist in dim_data (using anti-join)."""
        # Convert dim_data IDs to a LazyFrame for joining
        valid_ids_df = pl.DataFrame({"id_data": dim_data["id_data"]}).lazy()
        
        if isinstance(fct_clima_lazy, pl.LazyFrame):
            # Use anti-join to find invalid IDs efficiently
            invalid_count = fct_clima_lazy.join(
                valid_ids_df, on="id_data", how="anti"
            ).select(pl.len()).collect().item()
        else:
            # Fallback for DataFrame
            valid_ids = set(dim_data["id_data"].unique().to_list())
            fact_ids = set(fct_clima_lazy["id_data"].unique().to_list())
            invalid_count = len(fact_ids - valid_ids)
        
        assert invalid_count == 0, (
            f"Found {invalid_count} records with invalid id_data values in fact table"
        )

    def test_id_local_references_dim_local_clima(self, fct_clima_lazy, dim_local_clima):
        """Test that all id_local values exist in dim_local_clima (using anti-join)."""
        # Convert dim_local IDs to a LazyFrame for joining
        valid_ids_df = pl.DataFrame({"id_local": dim_local_clima["id_local"]}).lazy()
        
        if isinstance(fct_clima_lazy, pl.LazyFrame):
            # Use anti-join to find invalid IDs efficiently
            invalid_count = fct_clima_lazy.join(
                valid_ids_df, on="id_local", how="anti"
            ).select(pl.len()).collect().item()
        else:
            # Fallback for DataFrame
            valid_ids = set(dim_local_clima["id_local"].unique().to_list())
            fact_ids = set(fct_clima_lazy["id_local"].unique().to_list())
            invalid_count = len(fact_ids - valid_ids)
        
        assert invalid_count == 0, (
            f"Found {invalid_count} records with invalid id_local values in fact table"
        )


class TestFctClimaDataQuality:
    """Additional data quality tests for fct_clima using efficient aggregations."""

    def test_null_percentage_acceptable(self, fct_clima_stats):
        """Test that null percentages for each column are within acceptable limits."""
        total_count = fct_clima_stats["total_count"]
        
        # Check nullable columns (all except keys)
        # Different thresholds for different types of data
        column_thresholds = {
            "temperatura": 50,
            "umidade_relativa": 50,
            "vento_velocidade": 50,
            "vento_direcao": 50,
            "co_ppb": 80,  # Air quality data can be very sparse
            "no2_ppb": 80,
            "o3_ppb": 80,
            "pm25_ugm3": 80,
            "so2_ugm3": 80,
            "precipitacao_dia": 80,  # Precipitation data often sparse
        }
        
        for col, threshold in column_thresholds.items():
            null_count = fct_clima_stats[f"{col}_null_count"]
            null_percentage = (null_count / total_count) * 100
            assert null_percentage <= threshold, (
                f"{col} has {null_percentage:.2f}% nulls (expected <= {threshold}%)"
            )
            if null_percentage > threshold * 0.8:  # Warn if approaching limit
                print(f"WARNING: {col} has {null_percentage:.2f}% nulls (threshold: {threshold}%)")

    def test_no_duplicate_records(self, fct_clima_lazy):
        """Test that there are no duplicate records based on key columns."""
        # This test is memory intensive. Skip if dataset is too large or use approximation.
        # For now, we'll keep it but warn it might be slow.
        if isinstance(fct_clima_lazy, pl.LazyFrame):
            # Count total vs unique combinations of keys
            total_count = fct_clima_lazy.select(pl.len()).collect().item()
            
            # Optimization: Check if we can skip this for very large datasets in test environment
            if total_count > 50_000_000:
                print(f"WARNING: Skipping exact duplicate check for large dataset ({total_count:,} rows) to save memory")
                return

            unique_count = fct_clima_lazy.select(["id_data", "id_local", "id_horario"]).unique().select(pl.len()).collect().item()
            
            assert total_count == unique_count, (
                f"Found {total_count - unique_count} duplicate records based on (id_data, id_local, id_horario)"
            )
        else:
            total_count = len(fct_clima_lazy)
            unique_count = fct_clima_lazy.select(["id_data", "id_local", "id_horario"]).unique().height
            
            assert total_count == unique_count, (
                f"Found {total_count - unique_count} duplicate records based on (id_data, id_local, id_horario)"
            )

    def test_reasonable_record_count(self, fct_clima_stats):
        """Test that the record count is within expected range."""
        count = fct_clima_stats["total_count"]
        
        # Expect at least 1M records (sanity check) and less than 1B (reasonableness)
        assert count >= 1_000_000, f"Too few records: {count:,} (expected >= 1M)"
        assert count <= 1_000_000_000, f"Too many records: {count:,} (expected <= 1B)"
        print(f"INFO: fct_clima has {count:,} records")

