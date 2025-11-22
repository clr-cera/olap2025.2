"""Schema validation tests for dim_local_queimada."""
import polars as pl


class TestDimLocalQueimadaSchema:
    """Test suite for dim_local_queimada schema validation."""

    def test_columns_exist(self, dim_local_queimada):
        """Test that all expected columns exist."""
        expected_columns = {
            "id_local", "id_municipio", "nome_municipio", "sigla_uf",
            "nome_uf", "regiao_uf", "bioma", "latitude", "longitude"
        }
        actual_columns = set(dim_local_queimada.columns)
        assert expected_columns == actual_columns, (
            f"Column mismatch. Expected: {expected_columns}, Got: {actual_columns}"
        )

    def test_id_local_dtype(self, dim_local_queimada):
        """Test that id_local has correct data type."""
        assert dim_local_queimada["id_local"].dtype == pl.UInt32

    def test_id_municipio_dtype(self, dim_local_queimada):
        """Test that id_municipio has correct data type."""
        assert dim_local_queimada["id_municipio"].dtype == pl.Int32

    def test_nome_municipio_dtype(self, dim_local_queimada):
        """Test that nome_municipio has string type."""
        assert dim_local_queimada["nome_municipio"].dtype == pl.String

    def test_sigla_uf_dtype(self, dim_local_queimada):
        """Test that sigla_uf has string type."""
        assert dim_local_queimada["sigla_uf"].dtype == pl.String

    def test_nome_uf_dtype(self, dim_local_queimada):
        """Test that nome_uf has string type."""
        assert dim_local_queimada["nome_uf"].dtype == pl.String

    def test_regiao_uf_dtype(self, dim_local_queimada):
        """Test that regiao_uf has string type."""
        assert dim_local_queimada["regiao_uf"].dtype == pl.String

    def test_bioma_dtype(self, dim_local_queimada):
        """Test that bioma has string type."""
        assert dim_local_queimada["bioma"].dtype == pl.String

    def test_latitude_dtype(self, dim_local_queimada):
        """Test that latitude has float type."""
        assert dim_local_queimada["latitude"].dtype == pl.Float64

    def test_longitude_dtype(self, dim_local_queimada):
        """Test that longitude has float type."""
        assert dim_local_queimada["longitude"].dtype == pl.Float64

    def test_no_nulls_id_local(self, dim_local_queimada):
        """Test that id_local has no null values."""
        assert dim_local_queimada["id_local"].null_count() == 0

    def test_no_nulls_id_municipio(self, dim_local_queimada):
        """Test that id_municipio has no null values."""
        assert dim_local_queimada["id_municipio"].null_count() == 0

    def test_no_nulls_sigla_uf(self, dim_local_queimada):
        """Test that sigla_uf has no null values."""
        assert dim_local_queimada["sigla_uf"].null_count() == 0



    def test_id_local_unique(self, dim_local_queimada):
        """Test that id_local values are unique."""
        assert dim_local_queimada["id_local"].is_unique().all()

    def test_latitude_range(self, dim_local_queimada):
        """Test that latitude values are in valid range for Brazil [-34, 6]."""
        assert dim_local_queimada["latitude"].min() >= -34
        assert dim_local_queimada["latitude"].max() <= 6

    def test_longitude_range(self, dim_local_queimada):
        """Test that longitude values are in valid range for Brazil [-74, -34]."""
        assert dim_local_queimada["longitude"].min() >= -74
        assert dim_local_queimada["longitude"].max() <= -34

    def test_sigla_uf_length(self, dim_local_queimada):
        """Test that sigla_uf has length 2."""
        assert (dim_local_queimada["sigla_uf"].str.len_chars() == 2).all()

    def test_regiao_uf_valid_values(self, dim_local_queimada):
        """Test that regiao_uf contains only valid Brazilian regions."""
        valid_regioes = {"Norte", "Nordeste", "Centro-Oeste", "Sudeste", "Sul"}
        unique_regioes = set(dim_local_queimada["regiao_uf"].unique().to_list())
        # Remove None if present
        unique_regioes.discard(None)
        assert unique_regioes.issubset(valid_regioes), (
            f"Invalid regions found: {unique_regioes - valid_regioes}"
        )

    def test_bioma_valid_values(self, dim_local_queimada):
        """Test that bioma contains only valid Brazilian biomes."""
        valid_biomas = {
            "Amazônia", "Cerrado", "Mata Atlântica", "Caatinga",
            "Pampa", "Pantanal"
        }
        unique_biomas = set(dim_local_queimada["bioma"].unique().to_list())
        # Remove None if present
        unique_biomas.discard(None)
        assert unique_biomas.issubset(valid_biomas), (
            f"Invalid biomas found: {unique_biomas - valid_biomas}"
        )