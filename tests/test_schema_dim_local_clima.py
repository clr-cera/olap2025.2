"""Schema validation tests for dim_local_clima."""
import polars as pl


class TestDimLocalClimaSchema:
    """Test suite for dim_local_clima schema validation."""

    def test_columns_exist(self, dim_local_clima):
        """Test that all expected columns exist."""
        expected_columns = {
            "id_local", "id_municipio", "nome_municipio",
            "sigla_uf", "nome_uf", "regiao_uf"
        }
        actual_columns = set(dim_local_clima.columns)
        assert expected_columns == actual_columns, (
            f"Column mismatch. Expected: {expected_columns}, Got: {actual_columns}"
        )

    def test_id_local_dtype(self, dim_local_clima):
        """Test that id_local has correct data type."""
        assert dim_local_clima["id_local"].dtype == pl.UInt32

    def test_id_municipio_dtype(self, dim_local_clima):
        """Test that id_municipio has correct data type."""
        assert dim_local_clima["id_municipio"].dtype == pl.Int32

    def test_nome_municipio_dtype(self, dim_local_clima):
        """Test that nome_municipio has string type."""
        assert dim_local_clima["nome_municipio"].dtype == pl.String

    def test_sigla_uf_dtype(self, dim_local_clima):
        """Test that sigla_uf has string type."""
        assert dim_local_clima["sigla_uf"].dtype == pl.String

    def test_nome_uf_dtype(self, dim_local_clima):
        """Test that nome_uf has string type."""
        assert dim_local_clima["nome_uf"].dtype == pl.String

    def test_regiao_uf_dtype(self, dim_local_clima):
        """Test that regiao_uf has string type."""
        assert dim_local_clima["regiao_uf"].dtype == pl.String

    def test_no_nulls_id_local(self, dim_local_clima):
        """Test that id_local has no null values."""
        assert dim_local_clima["id_local"].null_count() == 0

    def test_no_nulls_id_municipio(self, dim_local_clima):
        """Test that id_municipio has no null values."""
        assert dim_local_clima["id_municipio"].null_count() == 0

    def test_no_nulls_sigla_uf(self, dim_local_clima):
        """Test that sigla_uf has no null values."""
        assert dim_local_clima["sigla_uf"].null_count() == 0

    def test_id_local_unique(self, dim_local_clima):
        """Test that id_local values are unique."""
        assert dim_local_clima["id_local"].is_unique().all()

    def test_municipio_uf_combination_unique(self, dim_local_clima):
        """Test that id_municipio and sigla_uf combination is unique."""
        assert dim_local_clima.select(["id_municipio", "sigla_uf"]).is_unique().all()

    def test_sigla_uf_length(self, dim_local_clima):
        """Test that sigla_uf has length 2."""
        assert (dim_local_clima["sigla_uf"].str.len_chars() == 2).all()

    def test_regiao_uf_valid_values(self, dim_local_clima):
        """Test that regiao_uf contains only valid Brazilian regions."""
        valid_regioes = {"Norte", "Nordeste", "Centro-Oeste", "Sudeste", "Sul"}
        unique_regioes = set(dim_local_clima["regiao_uf"].unique().to_list())
        # Remove None if present
        unique_regioes.discard(None)
        assert unique_regioes.issubset(valid_regioes), (
            f"Invalid regions found: {unique_regioes - valid_regioes}"
        )
