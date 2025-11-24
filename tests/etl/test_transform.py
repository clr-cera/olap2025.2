import pytest
from datetime import datetime
from pyspark.sql import Row
from etl.transform import Transformer

def test_transform_dim_horarios(spark):
    transformer = Transformer(spark)
    df = transformer.transform_dim_horarios()
    
    assert df.count() == 24 * 60
    assert "id_horario" in df.columns
    assert "hora" in df.columns
    assert "minuto" in df.columns

def test_transform_dim_data(spark):
    transformer = Transformer(spark)
    
    queimadas_data = [Row(data_hora=datetime(2023, 1, 1, 12, 0, 0))]
    clima_data = [Row(data_hora=datetime(2023, 6, 21, 12, 0, 0))]
    
    q_df = spark.createDataFrame(queimadas_data)
    c_df = spark.createDataFrame(clima_data)
    
    dim_data = transformer.transform_dim_data(q_df, c_df)
    
    assert dim_data.count() == 2
    
    row1 = dim_data.filter("mes = 1").first()
    assert row1.estacao == 1 # Summer
    assert row1.semestre == 1
    
    row2 = dim_data.filter("mes = 6").first()
    assert row2.estacao == 3 # Winter starts Jun 21
    assert row2.semestre == 1

def test_transform_dim_local_queimada(spark):
    transformer = Transformer(spark)
    
    queimadas_data = [Row(id_municipio=1, sigla_uf="SP", bioma="Mata Atlantica", latitude=1.0, longitude=1.0)]
    municipios_data = [Row(**{"CÓDIGO DO MUNICÍPIO - IBGE": 1, "MUNICÍPIO - IBGE": "Sao Paulo"})]
    uf_data = [Row(sigla="SP", nome="Sao Paulo", regiao="Sudeste")]
    
    q_df = spark.createDataFrame(queimadas_data)
    m_df = spark.createDataFrame(municipios_data)
    u_df = spark.createDataFrame(uf_data)
    
    dim_local = transformer.transform_dim_local_queimada(q_df, m_df, u_df)
    
    assert dim_local.count() == 1
    row = dim_local.first()
    assert row.nome_municipio == "Sao Paulo"
    assert row.nome_uf == "Sao Paulo"
    assert row.regiao_uf == "Sudeste"

def test_transform_fct_queimadas(spark):
    transformer = Transformer(spark)
    
    # Mock inputs
    queimadas_data = [Row(
        id_municipio=1, sigla_uf="SP", bioma="Mata", latitude=1.0, longitude=1.0, 
        data_hora=datetime(2023, 1, 1, 12, 30, 0),
        precipitacao=10.0, risco_fogo=5.0, potencia_radiativa_fogo=100.0, dias_sem_chuva=2
    )]
    
    dim_data_data = [Row(id_data=100, date_time_iso=datetime(2023, 1, 1, 12, 30, 0))]
    dim_local_data = [Row(id_local=200, id_municipio=1, sigla_uf="SP", latitude=1.0, longitude=1.0, bioma="Mata")]
    dim_horarios_data = [Row(id_horario=300, hora=12, minuto=30)]
    
    q_df = spark.createDataFrame(queimadas_data)
    d_df = spark.createDataFrame(dim_data_data)
    l_df = spark.createDataFrame(dim_local_data)
    h_df = spark.createDataFrame(dim_horarios_data)
    
    fct = transformer.transform_fct_queimadas(q_df, d_df, l_df, h_df)
    
    assert fct.count() == 1
    row = fct.first()
    assert row.id_data == 100
    assert row.id_local == 200
    assert row.id_horario == 300
    assert row.precipitacao == 10.0
