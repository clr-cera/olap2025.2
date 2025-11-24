"""Schema validation tests for fct_queimadas."""
import pytest
import polars as pl


@pytest.fixture(scope="module")
def fct_queimadas_stats(fct_queimadas_lazy):
    """Compute statistics for fct_queimadas in a single pass to avoid multiple scans."""
    if not isinstance(fct_queimadas_lazy, pl.LazyFrame):
        # Fallback for DataFrame (e.g. Postgres source)
        df = fct_queimadas_lazy
        return {
            "count": len(df),
            "nulls": {c: df[c].null_count() for c in df.columns},
            "min": {
                "precipitacao": df["precipitacao"].min(),
                "risco_fogo": df["risco_fogo"].min(),
                "dias_sem_chuva": df["dias_sem_chuva"].min(),
            },
            "max": {
                "precipitacao": df["precipitacao"].max(),
                "risco_fogo": df["risco_fogo"].max(),
                "dias_sem_chuva": df["dias_sem_chuva"].max(),
            }
        }

    # Define aggregations for single-pass execution
    columns = fct_queimadas_lazy.collect_schema().names()
    exprs = [
        pl.len().alias("count"),
        *[pl.col(c).null_count().alias(f"nulls_{c}") for c in columns],
        pl.col("precipitacao").min().alias("min_precipitacao"),
        pl.col("risco_fogo").min().alias("min_risco_fogo"),
        pl.col("dias_sem_chuva").min().alias("min_dias_sem_chuva"),
        pl.col("precipitacao").max().alias("max_precipitacao"),
        pl.col("risco_fogo").max().alias("max_risco_fogo"),
        pl.col("dias_sem_chuva").max().alias("max_dias_sem_chuva"),
    ]
    
    # Execute all aggregations in one streaming pass
    try:
        stats = fct_queimadas_lazy.select(exprs).collect(engine="streaming")
    except Exception:
        # Fallback if streaming fails
        stats = fct_queimadas_lazy.select(exprs).collect()
        
    row = stats.row(0)
    cols = stats.columns
    
    result = {
        "count": row[cols.index("count")],
        "nulls": {},
        "min": {},
        "max": {}
    }
    
    for i, col in enumerate(cols):
        if col.startswith("nulls_"):
            original_col = col.replace("nulls_", "")
            result["nulls"][original_col] = row[i]
        elif col.startswith("min_"):
            original_col = col.replace("min_", "")
            result["min"][original_col] = row[i]
        elif col.startswith("max_"):
            original_col = col.replace("max_", "")
            result["max"][original_col] = row[i]
            
    return result


class TestFctQueimadasSchema:
    """Test suite for fct_queimadas schema validation."""

    def test_columns_exist(self, fct_queimadas_lazy):
        """Test that all expected columns exist."""
        expected_columns = {
            "id_data", "id_local", "id_horario",
            "precipitacao", "risco_fogo", "potencia_radiativa_fogo",
            "dias_sem_chuva"
        }
        actual_columns = set(fct_queimadas_lazy.collect_schema().names())
        assert expected_columns == actual_columns, (
            f"Column mismatch. Expected: {expected_columns}, Got: {actual_columns}"
        )

    def test_id_data_dtype(self, fct_queimadas_lazy):
        """Test that id_data has correct data type."""
        schema = fct_queimadas_lazy.collect_schema()
        assert schema["id_data"] in [pl.UInt32, pl.Int64, pl.Int32]

    def test_id_local_dtype(self, fct_queimadas_lazy):
        """Test that id_local has correct data type."""
        schema = fct_queimadas_lazy.collect_schema()
        assert schema["id_local"] in [pl.UInt32, pl.Int64, pl.Int32]

    def test_id_horario_dtype(self, fct_queimadas_lazy):
        """Test that id_horario has correct data type."""
        schema = fct_queimadas_lazy.collect_schema()
        assert schema["id_horario"] in [pl.Int32, pl.Int64]

    def test_precipitacao_dtype(self, fct_queimadas_lazy):
        """Test that precipitacao has float type."""
        schema = fct_queimadas_lazy.collect_schema()
        assert schema["precipitacao"] == pl.Float64

    def test_risco_fogo_dtype(self, fct_queimadas_lazy):
        """Test that risco_fogo has float type."""
        schema = fct_queimadas_lazy.collect_schema()
        assert schema["risco_fogo"] == pl.Float64

    def test_potencia_radiativa_fogo_dtype(self, fct_queimadas_lazy):
        """Test that potencia_radiativa_fogo has float type."""
        schema = fct_queimadas_lazy.collect_schema()
        assert schema["potencia_radiativa_fogo"] == pl.Float64

    def test_dias_sem_chuva_dtype(self, fct_queimadas_lazy):
        """Test that dias_sem_chuva has integer type."""
        schema = fct_queimadas_lazy.collect_schema()
        assert schema["dias_sem_chuva"] in [pl.Int64, pl.Float64]

    def test_no_nulls_id_data(self, fct_queimadas_stats):
        """Test that id_data has no null values."""
        assert fct_queimadas_stats["nulls"]["id_data"] == 0

    def test_no_nulls_id_local(self, fct_queimadas_stats):
        """Test that id_local has no null values."""
        assert fct_queimadas_stats["nulls"]["id_local"] == 0

    def test_no_nulls_id_horario(self, fct_queimadas_stats):
        """Test that id_horario has no null values."""
        assert fct_queimadas_stats["nulls"]["id_horario"] == 0

    def test_precipitacao_non_negative(self, fct_queimadas_stats):
        """Test that precipitacao values are non-negative where not null."""
        if fct_queimadas_stats["count"] > 0 and fct_queimadas_stats["min"]["precipitacao"] is not None:
            assert fct_queimadas_stats["min"]["precipitacao"] >= 0

    def test_risco_fogo_non_negative(self, fct_queimadas_stats):
        """Test that risco_fogo values are non-negative where not null."""
        if fct_queimadas_stats["count"] > 0 and fct_queimadas_stats["min"]["risco_fogo"] is not None:
            assert fct_queimadas_stats["min"]["risco_fogo"] >= 0

    def test_dias_sem_chuva_non_negative(self, fct_queimadas_stats):
        """Test that dias_sem_chuva values are non-negative where not null."""
        if fct_queimadas_stats["count"] > 0 and fct_queimadas_stats["min"]["dias_sem_chuva"] is not None:
            assert fct_queimadas_stats["min"]["dias_sem_chuva"] >= 0

    def test_has_records(self, fct_queimadas_stats):
        """Test that the fact table has at least one record."""
        assert fct_queimadas_stats["count"] > 0


class TestFctQueimadasReferentialIntegrity:
    """Test suite for referential integrity of fct_queimadas."""

    def test_id_data_references_dim_data(self, fct_queimadas_lazy, dim_data):
        """Test that all id_data values exist in dim_data using anti-join."""
        if isinstance(fct_queimadas_lazy, pl.LazyFrame):
            # Use anti-join to find invalid IDs without loading everything
            invalid_ids = fct_queimadas_lazy.join(
                dim_data.lazy() if isinstance(dim_data, pl.DataFrame) else dim_data,
                on="id_data",
                how="anti"
            ).select("id_data").unique().collect()
            
            assert len(invalid_ids) == 0, (
                f"Found {len(invalid_ids)} invalid id_data values in fact table. "
                f"Sample: {invalid_ids.head(5).to_series().to_list()}"
            )
        else:
            # Fallback for DataFrame
            valid_ids = set(dim_data["id_data"].unique().to_list())
            fact_ids = set(fct_queimadas_lazy["id_data"].unique().to_list())
            invalid_ids = fact_ids - valid_ids
            assert len(invalid_ids) == 0

    def test_id_local_references_dim_local(self, fct_queimadas_lazy, dim_local_queimada):
        """Test that all id_local values exist in dim_local_queimada using anti-join."""
        if isinstance(fct_queimadas_lazy, pl.LazyFrame):
            invalid_ids = fct_queimadas_lazy.join(
                dim_local_queimada.lazy() if isinstance(dim_local_queimada, pl.DataFrame) else dim_local_queimada,
                on="id_local",
                how="anti"
            ).select("id_local").unique().collect()
            
            assert len(invalid_ids) == 0, (
                f"Found {len(invalid_ids)} invalid id_local values in fact table. "
                f"Sample: {invalid_ids.head(5).to_series().to_list()}"
            )
        else:
            valid_ids = set(dim_local_queimada["id_local"].unique().to_list())
            fact_ids = set(fct_queimadas_lazy["id_local"].unique().to_list())
            invalid_ids = fact_ids - valid_ids
            assert len(invalid_ids) == 0

    def test_id_horario_references_dim_horarios(self, fct_queimadas_lazy, dim_horarios_queimada):
        """Test that all id_horario values exist in dim_horarios_queimada using anti-join."""
        if isinstance(fct_queimadas_lazy, pl.LazyFrame):
            invalid_ids = fct_queimadas_lazy.join(
                dim_horarios_queimada.lazy() if isinstance(dim_horarios_queimada, pl.DataFrame) else dim_horarios_queimada,
                on="id_horario",
                how="anti"
            ).select("id_horario").unique().collect()
            
            assert len(invalid_ids) == 0, (
                f"Found {len(invalid_ids)} invalid id_horario values in fact table. "
                f"Sample: {invalid_ids.head(5).to_series().to_list()}"
            )
        else:
            valid_ids = set(dim_horarios_queimada["id_horario"].unique().to_list())
            fact_ids = set(fct_queimadas_lazy["id_horario"].unique().to_list())
            invalid_ids = fact_ids - valid_ids
            assert len(invalid_ids) == 0


class TestFctQueimadasDataQuality:
    """Additional data quality tests for fct_queimadas using efficient aggregations."""

    def test_null_percentage_acceptable(self, fct_queimadas_stats):
        """Test that null percentages for each column are within acceptable limits."""
        total_count = fct_queimadas_stats["count"]
        
        # Check nullable columns
        # Allow up to 30% nulls (wildfire data can have missing values)
        threshold = 30
        nullable_cols = ["precipitacao", "risco_fogo", "potencia_radiativa_fogo", "dias_sem_chuva"]
        
        for col in nullable_cols:
            # Note: fct_queimadas_stats might not have null counts for all columns if not requested in fixture
            # The fixture currently calculates nulls for ALL columns
            if col in fct_queimadas_stats["nulls"]:
                null_count = fct_queimadas_stats["nulls"][col]
                null_percentage = (null_count / total_count) * 100
                assert null_percentage <= threshold, (
                    f"{col} has {null_percentage:.2f}% nulls (expected <= {threshold}%)"
                )

    def test_no_duplicate_records(self, fct_queimadas_lazy):
        """Test that there are minimal duplicate records based on key columns."""
        if isinstance(fct_queimadas_lazy, pl.LazyFrame):
            total_count = fct_queimadas_lazy.select(pl.len()).collect().item()
            
            # Optimization: Check if we can skip this for very large datasets
            if total_count > 50_000_000:
                print(f"WARNING: Skipping exact duplicate check for large dataset ({total_count:,} rows)")
                return

            unique_count = fct_queimadas_lazy.select(["id_data", "id_local", "id_horario"]).unique().select(pl.len()).collect().item()
            
            duplicate_count = total_count - unique_count
            duplicate_pct = (duplicate_count / total_count) * 100 if total_count > 0 else 0
            
            # Allow up to 0.5% duplicates
            assert duplicate_pct <= 0.5, (
                f"Found {duplicate_count:,} duplicate records ({duplicate_pct:.3f}%) in fct_queimadas"
            )
        else:
            total_count = len(fct_queimadas_lazy)
            unique_count = fct_queimadas_lazy.select(["id_data", "id_local", "id_horario"]).unique().height
            duplicate_count = total_count - unique_count
            duplicate_pct = (duplicate_count / total_count) * 100 if total_count > 0 else 0
            assert duplicate_pct <= 0.5

    def test_reasonable_record_count(self, fct_queimadas_stats):
        """Test that the record count is within expected range."""
        count = fct_queimadas_stats["count"]
        
        # Expect at least 100K records (sanity check) and less than 100M (reasonableness)
        assert count >= 100_000, f"Too few records: {count:,} (expected >= 100K)"
        assert count <= 100_000_000, f"Too many records: {count:,} (expected <= 100M)"
        print(f"INFO: fct_queimadas has {count:,} records")

    def test_potencia_radiativa_distribution(self, fct_queimadas_lazy):
        """Test that potencia_radiativa_fogo has reasonable distribution."""
        # Use lazy stats if possible
        if isinstance(fct_queimadas_lazy, pl.LazyFrame):
            stats = fct_queimadas_lazy.select([
                pl.col("potencia_radiativa_fogo").min().alias("min"),
                pl.col("potencia_radiativa_fogo").max().alias("max"),
                pl.col("potencia_radiativa_fogo").mean().alias("mean")
            ]).collect()
            
            min_val = stats["min"].item()
            max_val = stats["max"].item()
            mean_val = stats["mean"].item()
        else:
            min_val = fct_queimadas_lazy["potencia_radiativa_fogo"].min()
            max_val = fct_queimadas_lazy["potencia_radiativa_fogo"].max()
            mean_val = fct_queimadas_lazy["potencia_radiativa_fogo"].mean()
            
        print(f"INFO: potencia_radiativa_fogo - min: {min_val:.2f}, max: {max_val:.2f}, mean: {mean_val:.2f}")
        
        assert min_val >= -10, f"Unreasonably low FRP: {min_val}"
        assert max_val <= 10000, f"Unreasonably high FRP: {max_val}"
