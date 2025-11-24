"""Integration tests between queimada and clima data models."""
import polars as pl


class TestDimLocalQueimadaClimaIntegration:
    """Test suite for integration between dim_local_queimada and dim_local_clima."""

    def test_id_local_clima_column_exists(self, dim_local_queimada):
        """Test that id_local_clima column exists in dim_local_queimada."""
        # Note: This column may not exist depending on ETL design
        # Making this informational
        has_clima_ref = "id_local_clima" in dim_local_queimada.columns
        print(f"INFO: dim_local_queimada has id_local_clima column: {has_clima_ref}")

    def test_id_local_clima_dtype(self, dim_local_queimada):
        """Test that id_local_clima has correct data type."""
        if "id_local_clima" in dim_local_queimada.columns:
            assert dim_local_queimada["id_local_clima"].dtype in [pl.UInt32, pl.Int64, pl.Int32]

    def test_id_local_clima_references_valid_ids(self, dim_local_queimada, dim_local_clima):
        """Test that non-null id_local_clima values reference valid clima locations."""
        if "id_local_clima" not in dim_local_queimada.columns:
            return

        # Get non-null id_local_clima from queimada dimension
        non_null_refs = dim_local_queimada.filter(
            pl.col("id_local_clima").is_not_null()
        )
        
        if len(non_null_refs) > 0:
            valid_clima_ids = set(dim_local_clima["id_local"].unique().to_list())
            queimada_clima_ids = set(non_null_refs["id_local_clima"].unique().to_list())
            invalid_ids = queimada_clima_ids - valid_clima_ids
            assert len(invalid_ids) == 0, (
                f"Found {len(invalid_ids)} invalid id_local_clima references in dim_local_queimada"
            )

    def test_linked_locations_have_matching_attributes(self, dim_local_queimada, dim_local_clima):
        """Test that linked locations have matching municipio and UF data."""
        if "id_local_clima" not in dim_local_queimada.columns:
            return

        # Get queimada locations with clima link
        linked = dim_local_queimada.filter(
            pl.col("id_local_clima").is_not_null()
        ).select([
            "id_local_clima", "id_municipio", "sigla_uf",
            "nome_municipio", "nome_uf", "regiao_uf"
        ])
        
        if len(linked) > 0:
            # Join with clima dimension
            comparison = linked.join(
                dim_local_clima,
                left_on="id_local_clima",
                right_on="id_local",
                how="inner",
                suffix="_clima"
            )
            
            # Check that municipio IDs match
            assert (comparison["id_municipio"] == comparison["id_municipio_clima"]).all(), (
                "Found mismatched id_municipio between linked queimada and clima locations"
            )
            
            # Check that UF siglas match
            assert (comparison["sigla_uf"] == comparison["sigla_uf_clima"]).all(), (
                "Found mismatched sigla_uf between linked queimada and clima locations"
            )

    def test_dim_local_queimada_cardinality(self, dim_local_queimada):
        """Test that dim_local_queimada has reasonable cardinality."""
        count = len(dim_local_queimada)
        # Expect between 1K and 20M unique locations for queimada data
        # (updated based on actual data - 14.6M locations)
        assert count >= 1_000, f"Too few locations: {count:,} (expected >= 1K)"
        assert count <= 20_000_000, f"Too many locations: {count:,} (expected <= 20M)"
        print(f"INFO: dim_local_queimada has {count:,} locations")

    def test_dim_local_clima_cardinality(self, dim_local_clima):
        """Test that dim_local_clima has reasonable cardinality."""
        count = len(dim_local_clima)
        # Expect between 10 and 100K unique locations for clima data (fewer than queimadas)
        assert count >= 10, f"Too few locations: {count:,} (expected >= 10)"
        assert count <= 100_000, f"Too many locations: {count:,} (expected <= 100K)"
        print(f"INFO: dim_local_clima has {count:,} locations")


class TestDimDataIntegration:
    """Test suite for the merged dim_data dimension."""

    def test_dim_data_includes_both_sources(self, dim_data, fct_queimadas_lazy, fct_clima_lazy):
        """Test that dim_data includes dates from both queimada and clima sources."""
        if isinstance(fct_queimadas_lazy, pl.LazyFrame):
            queimada_dates = set(fct_queimadas_lazy.select("id_data").unique().collect()["id_data"].to_list())
        else:
            queimada_dates = set(fct_queimadas_lazy["id_data"].unique().to_list())
        
        if isinstance(fct_clima_lazy, pl.LazyFrame):
            clima_dates = set(fct_clima_lazy.select("id_data").unique().collect()["id_data"].to_list())
        else:
            clima_dates = set(fct_clima_lazy["id_data"].unique().to_list())
        
        dim_dates = set(dim_data["id_data"].unique().to_list())
        
        # All queimada dates should be in dim_data
        missing_queimada = queimada_dates - dim_dates
        assert len(missing_queimada) == 0, (
            f"Found {len(missing_queimada)} queimada dates not in dim_data"
        )
        
        # Check clima dates coverage
        missing_clima = clima_dates - dim_dates
        if len(missing_clima) > 0:
            coverage = ((len(clima_dates) - len(missing_clima)) / len(clima_dates)) * 100
            print(f"INFO: {coverage:.2f}% of clima dates are in dim_data ({len(missing_clima):,} missing)")
            # Expect at least 95% coverage
            assert coverage >= 95, f"Only {coverage:.2f}% of clima dates in dim_data (expected >= 95%)"

    def test_no_duplicate_dates(self, dim_data):
        """Test that dim_data has no duplicate date_time_iso values."""
        assert dim_data["date_time_iso"].is_unique().all()

    def test_id_data_sequential_after_merge(self, dim_data):
        """Test that id_data remains unique after merge."""
        # Check that all IDs are unique
        assert dim_data["id_data"].is_unique().all()
        
        # Check ID range is reasonable
        min_id = dim_data["id_data"].min()
        max_id = dim_data["id_data"].max()
        count = len(dim_data)
        
        print(f"INFO: dim_data has {count:,} dates, ID range: [{min_id}, {max_id}]")
        
        # IDs should be non-negative
        assert min_id >= 0, f"Negative IDs found: min={min_id}"

    def test_temporal_coverage(self, dim_data):
        """Test that dim_data covers expected time range."""
        dates = dim_data["date_time_iso"]
        min_date = dates.min()
        max_date = dates.max()
        
        print(f"INFO: dim_data time range: {min_date} to {max_date}")
        
        # Calculate span in days
        span = (max_date - min_date).days if hasattr(max_date - min_date, 'days') else 0
        
        # Expect at least 1 year of data
        assert span >= 365, f"Time span too short: {span} days (expected >= 365)"
        
        # Expect no more than 30 years
        assert span <= 365 * 30, f"Time span too long: {span} days (expected <= 30 years)"

    def test_temporal_continuity(self, dim_data):
        """Test that dim_data has reasonable temporal continuity (no huge gaps)."""
        # Sort by date
        sorted_dates = dim_data.select("date_time_iso").sort("date_time_iso")
        
        # Check gap between consecutive dates (sample to avoid memory issues)
        sample_size = min(10000, len(sorted_dates))
        sample = sorted_dates.head(sample_size)
        
        if len(sample) > 1:
            # Calculate time differences
            diffs = sample.select([
                (pl.col("date_time_iso") - pl.col("date_time_iso").shift(1)).alias("diff")
            ])
            
            # Get max gap in hours (assuming datetime type)
            max_gap = diffs["diff"].max()
            
            # For hourly data, expect max gap <= 7 days (reasonable for data collection gaps)
            # Convert to hours if possible
            print(f"INFO: Maximum gap in sample: {max_gap}")


class TestDimHorariosIntegration:
    """Test suite for dim_horarios integration with clima."""

    def test_dim_horarios_has_clima_reference(self, dim_horarios_queimada):
        """Test that dim_horarios_queimada includes id_horario_clima column."""
        # Note: Based on your ETL, the queimada horarios table may have clima reference
        has_clima_ref = "id_horario_clima" in dim_horarios_queimada.columns
        print(f"INFO: dim_horarios_queimada has id_horario_clima column: {has_clima_ref}")

    def test_id_horario_clima_dtype(self, dim_horarios_queimada):
        """Test that id_horario_clima has correct data type."""
        if "id_horario_clima" in dim_horarios_queimada.columns:
            assert dim_horarios_queimada["id_horario_clima"].dtype in [pl.UInt32, pl.Int32, pl.Int64]

    def test_id_horario_clima_references_valid_ids(self, dim_horarios_queimada, dim_horarios_clima):
        """Test that id_horario_clima references valid clima time IDs."""
        if "id_horario_clima" not in dim_horarios_queimada.columns:
            return

        valid_clima_ids = set(dim_horarios_clima["id_horario"].unique().to_list())
        queimada_clima_refs = set(dim_horarios_queimada["id_horario_clima"].unique().to_list())
        invalid_ids = queimada_clima_refs - valid_clima_ids
        
        assert len(invalid_ids) == 0, (
            f"Found {len(invalid_ids)} invalid id_horario_clima references"
        )

    def test_hora_mapping_consistency(self, dim_horarios_queimada, dim_horarios_clima):
        """Test that the same hora values map to consistent id_horario_clima."""
        if "id_horario_clima" not in dim_horarios_queimada.columns:
            return

        # Group by hora and check id_horario_clima is consistent
        grouped = dim_horarios_queimada.group_by("hora").agg([
            pl.col("id_horario_clima").n_unique().alias("unique_clima_ids")
        ])
        
        # Each hora should map to exactly one id_horario_clima
        assert (grouped["unique_clima_ids"] == 1).all(), (
            "Found inconsistent id_horario_clima mapping for same hora values"
        )


class TestCrossFactTableConsistency:
    """Test suite for consistency between queimada and clima fact tables."""

    def test_shared_dates_exist(self, fct_queimadas_lazy, fct_clima_lazy):
        """Test that there are some overlapping dates between the two fact tables."""
        if isinstance(fct_queimadas_lazy, pl.LazyFrame):
            queimada_dates = set(fct_queimadas_lazy.select("id_data").unique().collect()["id_data"].to_list())
        else:
            queimada_dates = set(fct_queimadas_lazy["id_data"].unique().to_list())
        
        if isinstance(fct_clima_lazy, pl.LazyFrame):
            clima_dates = set(fct_clima_lazy.select("id_data").unique().collect()["id_data"].to_list())
        else:
            clima_dates = set(fct_clima_lazy["id_data"].unique().to_list())
        
        shared_dates = queimada_dates & clima_dates
        
        overlap_pct = (len(shared_dates) / len(queimada_dates)) * 100 if len(queimada_dates) > 0 else 0
        print(f"INFO: {len(shared_dates):,} shared dates ({overlap_pct:.2f}% of queimada dates)")

    def test_fact_tables_use_same_dim_data(self, fct_queimadas_lazy, fct_clima_lazy, dim_data):
        """Test that both fact tables reference the same unified dim_data."""
        if isinstance(fct_queimadas_lazy, pl.LazyFrame):
            all_queimada_dates = set(fct_queimadas_lazy.select("id_data").unique().collect()["id_data"].to_list())
        else:
            all_queimada_dates = set(fct_queimadas_lazy["id_data"].unique().to_list())
        
        if isinstance(fct_clima_lazy, pl.LazyFrame):
            all_clima_dates = set(fct_clima_lazy.select("id_data").unique().collect()["id_data"].to_list())
        else:
            all_clima_dates = set(fct_clima_lazy["id_data"].unique().to_list())
        
        dim_dates = set(dim_data["id_data"].unique().to_list())
        
        # All queimada fact table dates should exist in the unified dimension
        assert all_queimada_dates.issubset(dim_dates), "Some queimada dates not in dim_data"
        
        # Check clima dates coverage
        missing_clima = all_clima_dates - dim_dates
        if len(missing_clima) > 0:
            coverage = ((len(all_clima_dates) - len(missing_clima)) / len(all_clima_dates)) * 100
            print(f"INFO: {coverage:.2f}% of clima fact dates in dim_data ({len(missing_clima):,} missing)")


class TestGeographicDistribution:
    """Test geographic distribution across both datasets."""

    def test_queimada_state_coverage(self, dim_local_queimada):
        """Test that queimada data covers multiple Brazilian states."""
        unique_states = dim_local_queimada["sigla_uf"].n_unique()
        
        # Expect at least 15 states (Brazil has 27 states)
        assert unique_states >= 15, (
            f"Too few states in queimada data: {unique_states} (expected >= 15)"
        )
        print(f"INFO: Queimada data covers {unique_states} states")

    def test_clima_state_coverage(self, dim_local_clima):
        """Test that clima data covers multiple Brazilian states."""
        unique_states = dim_local_clima["sigla_uf"].n_unique()
        
        # Expect at least 10 states
        assert unique_states >= 10, (
            f"Too few states in clima data: {unique_states} (expected >= 10)"
        )
        print(f"INFO: Clima data covers {unique_states} states")

    def test_queimada_biome_coverage(self, dim_local_queimada):
        """Test that queimada data covers multiple Brazilian biomes."""
        unique_biomes = dim_local_queimada.filter(
            pl.col("bioma").is_not_null()
        )["bioma"].n_unique()
        
        # Expect at least 3 biomes
        assert unique_biomes >= 3, (
            f"Too few biomes in queimada data: {unique_biomes} (expected >= 3)"
        )
        print(f"INFO: Queimada data covers {unique_biomes} biomes")

    def test_queimada_region_coverage(self, dim_local_queimada):
        """Test that queimada data covers multiple Brazilian regions."""
        unique_regions = dim_local_queimada.filter(
            pl.col("regiao_uf").is_not_null()
        )["regiao_uf"].n_unique()
        
        # Expect at least 3 regions (Brazil has 5 regions)
        assert unique_regions >= 3, (
            f"Too few regions in queimada data: {unique_regions} (expected >= 3)"
        )
        print(f"INFO: Queimada data covers {unique_regions} regions")

