# %%
import polars as pl

# %%
queimadas_df = pl.read_parquet("data/queimadas-full.pqt.zstd")
municipios_df = pl.read_csv("data/municipios.csv", separator=";",encoding='iso-8859-1')
uf_df = pl.read_csv("data/uf.csv")

# %%
municipios_df.head()

# %%
uf_df.head()

# %%
queimadas_df = queimadas_df.filter(pl.col('bioma').is_not_null())
queimadas_df = queimadas_df.with_columns([pl.col('dias_sem_chuva').cast(pl.Int64)])
queimadas_df = queimadas_df.with_columns([pl.col('dias_sem_chuva').replace(-999,None)])
queimadas_df = queimadas_df.with_columns([pl.col('risco_fogo').replace(-999,None)])
queimadas_df

# %%
# generate dim_horarios_queimada
# generate all combinations of hour and minutes in a day
minuto_list = 24*[list(range(0, 60))]
dim_horarios_full = pl.DataFrame({
    "hora": list(range(0, 24)),
    "minuto": minuto_list}).explode("minuto").with_row_index("id_horario").select([
        pl.col("id_horario").cast(pl.Int32),
        pl.col("hora").cast(pl.Int8),
        pl.col("minuto").cast(pl.Int8)
    ])
dim_horarios_full

# %%
# create dim_data_queimada
dim_data_queimada = (queimadas_df
    .select([pl.col("data_hora").alias("date_time_iso"),
             pl.col("data_hora").dt.day().alias("dia"), 
             pl.col("data_hora").dt.month().alias("mes"), 
             pl.col("data_hora").dt.year().alias("ano"),
             # generate semester column
             pl.col("data_hora").dt.month().map_elements(lambda x: 1 if x <= 6 else 2).alias("semestre"),
             # generate trimester column
             ((pl.col("data_hora").dt.month() - 1 )//3 + 1).alias("trimestre"),
             # generate week day column
             pl.col("data_hora").dt.weekday().alias("dia_semana"),
             # generate day of year column
             pl.col("data_hora").dt.ordinal_day().alias("dia_ano"),
             # generate is_weekend column, starting from saturday (6)
             pl.col("data_hora").dt.weekday().map_elements(lambda x: x >= 6).alias("is_weekend"),
             # generate week of year column
             pl.col("data_hora").dt.week().alias("semana_ano")
             ]).unique().sort("date_time_iso")).with_row_index("id_data")
print(len(dim_data_queimada))
dim_data_queimada

# %%
# add season to dim_data_queimada using day and month
# AAAAAAAAAAAAAA there is no built-in season function in polars
# so we have to do it manually
# Summer - 1: Dec 21 - Mar 19
# Autumn - 2: Mar 20 - Jun 20
# Winter - 3: Jun 21 - Sep 21
# Spring - 4: Sep 22 - Dec 20
dim_data_queimada = dim_data_queimada.with_columns(
    pl.when(( (pl.col("mes") == 12) & (pl.col("dia") >= 21) ) | (pl.col("mes").is_in([1,2])) | ((pl.col("mes") == 3) & (pl.col("dia") < 20)))
    .then(1)
    .when(( (pl.col("mes") == 3) & (pl.col("dia") >= 20) ) | (pl.col("mes").is_in([4,5])) | ((pl.col("mes") == 6) & (pl.col("dia") < 21)))
    .then(2)
    .when(( (pl.col("mes") == 6) & (pl.col("dia") >= 21) ) | (pl.col("mes").is_in([7,8])) | ((pl.col("mes") == 9) & (pl.col("dia") < 22)))
    .then(3)
    .otherwise(4).alias("estacao")
)
dim_data_queimada.head()

# %%
# generate dim_local_queimada
dim_local_queimada = (queimadas_df
    .select([pl.col("id_municipio").alias("id_municipio").cast(pl.Int32),
             pl.col("sigla_uf").alias("sigla_uf"),
             pl.col("bioma").alias("bioma"),
             pl.col("latitude").alias("latitude"),
             pl.col("longitude").alias("longitude"),]).unique().sort(["id_municipio", "sigla_uf"]))
dim_local_queimada.head()

# %%
# join with municipios to get municipio name
# join with uf to get uf name and regiao
dim_local_queimada = (dim_local_queimada
    .join(municipios_df.select([pl.col("MUNICÍPIO - IBGE"), pl.col('CÓDIGO DO MUNICÍPIO - IBGE')]), left_on="id_municipio", right_on="CÓDIGO DO MUNICÍPIO - IBGE", how="left")
    .join(uf_df.select([pl.col("sigla"), pl.col("nome"), pl.col("regiao")]), left_on="sigla_uf", right_on="sigla", how="left"))
dim_local_queimada = dim_local_queimada.select([
    pl.col("id_municipio"),
    pl.col("MUNICÍPIO - IBGE").alias("nome_municipio"),
    pl.col("sigla_uf"),
    pl.col("nome").alias("nome_uf"),
    pl.col("regiao").alias("regiao_uf"),
    pl.col("bioma"),
    pl.col("latitude"),
    pl.col("longitude"),
]).sort(["id_municipio", "sigla_uf"]).unique().with_row_index("id_local")
print(len(dim_local_queimada))
dim_local_queimada

# %%
# generate fct_queimadas

# join by date_time
fct_queimadas = (queimadas_df.join(
    dim_data_queimada.select([pl.col("date_time_iso"), pl.col('id_data')]),
    left_on="data_hora", right_on="date_time_iso", how="left"
).with_columns([pl.col('id_municipio').cast(pl.Int32)])
# join by id_municipio, sigla_uf, latitude, longitude, and bioma
.join(
    dim_local_queimada.select([pl.col("id_local"), pl.col("id_municipio"), pl.col("sigla_uf"), pl.col("latitude"), pl.col("longitude"), pl.col("bioma")]),
    left_on=["id_municipio", "sigla_uf", "latitude", "longitude", "bioma"],
    right_on=["id_municipio", "sigla_uf", "latitude", "longitude", "bioma"],
    how="left"
)
# join dim horarios_queimada to get id_horario
.join(
    dim_horarios_full.select([pl.col("id_horario"), pl.col("hora"), pl.col("minuto")]),
    left_on=[pl.col("data_hora").dt.hour(), pl.col("data_hora").dt.minute()],
    right_on=["hora", "minuto"],
    how="left")
.select([
    pl.col("id_data"),
    pl.col("id_local"),
    pl.col("id_horario"),
    pl.col('precipitacao').alias('precipitacao'),
    pl.col('risco_fogo').alias('risco_fogo'),
    pl.col('potencia_radiativa_fogo').alias('potencia_radiativa_fogo'),
    pl.col('dias_sem_chuva').alias('dias_sem_chuva')
])).unique()
    
print(len(fct_queimadas))
fct_queimadas

# %%
# load to parquet
dim_horarios_full.write_parquet("data/dim_horarios_queimada.pqt.zstd", compression="zstd")
dim_local_queimada.write_parquet("data/dim_local_queimada.pqt.zstd", compression="zstd")
dim_data_queimada.write_parquet("data/dim_data.pqt.zstd", compression="zstd")
fct_queimadas.write_parquet("data/fct_queimadas.pqt.zstd", compression="zstd")

# %%
clima_df = pl.read_parquet('data/sisam-full.pqt.zstd')
clima_df = clima_df.filter(pl.col('sigla_uf').is_not_null() & pl.col('id_municipio').is_not_null() )
clima_df

# %%
# insert data_hora from clima_df into dim_data_queimada

dim_data_clima = (clima_df
    .select([pl.col("data_hora").alias("date_time_iso"),
             pl.col("data_hora").dt.day().alias("dia"), 
             pl.col("data_hora").dt.month().alias("mes"), 
             pl.col("data_hora").dt.year().alias("ano"),
             # generate semester column
             pl.col("data_hora").dt.month().map_elements(lambda x: 1 if x <= 6 else 2).alias("semestre"),
             # generate trimester column
             ((pl.col("data_hora").dt.month() - 1 )//3 + 1).alias("trimestre"),
             # generate week day column
             pl.col("data_hora").dt.weekday().alias("dia_semana"),
             # generate day of year column
             pl.col("data_hora").dt.ordinal_day().alias("dia_ano"),
             # generate is_weekend column, starting from saturday (6)
             pl.col("data_hora").dt.weekday().map_elements(lambda x: x >= 6).alias("is_weekend"),
             # generate week of year column
             pl.col("data_hora").dt.week().alias("semana_ano")
             ]).unique().sort("date_time_iso")).with_row_index("id_data")
dim_data_clima

# %%

dim_data_clima = dim_data_clima.with_columns(
    pl.when(( (pl.col("mes") == 12) & (pl.col("dia") >= 21) ) | (pl.col("mes").is_in([1,2])) | ((pl.col("mes") == 3) & (pl.col("dia") < 20)))
    .then(1)
    .when(( (pl.col("mes") == 3) & (pl.col("dia") >= 20) ) | (pl.col("mes").is_in([4,5])) | ((pl.col("mes") == 6) & (pl.col("dia") < 21)))
    .then(2)
    .when(( (pl.col("mes") == 6) & (pl.col("dia") >= 21) ) | (pl.col("mes").is_in([7,8])) | ((pl.col("mes") == 9) & (pl.col("dia") < 22)))
    .then(3)
    .otherwise(4).alias("estacao")
)
dim_data_clima.head(1000)

# %%
# insert tuples from dim_data_clima into dim_data_queimada, avoiding duplicates and increasing id_data accordingly
max_id_data = dim_data_queimada.select(pl.col("id_data").max()).item()
dim_data_clima = dim_data_clima.with_columns(pl.col('id_data')+ max_id_data + 1)
print(max_id_data)
new_data = dim_data_clima.join(
    dim_data_queimada.select("date_time_iso"),
    on="date_time_iso",
    how="anti"
)
print(len(new_data))
dim_data = pl.concat([dim_data_queimada, new_data]).sort("id_data")
dim_data

# %%
# generate dim_horario_clima

dim_horarios_clima = pl.DataFrame({
    "hora": list(range(0, 24)),}).with_row_index("id_horario")
dim_horarios_clima

# %%
# link dim_horario_clima to dim_horarios_full
dim_horarios_full = dim_horarios_full.join(
    dim_horarios_clima,
    on="hora",
    how="inner"
).rename({"id_horario_right": "id_horario_clima"})
dim_horarios_full

# %%
# generate dim_local_clima

dim_local_clima = (clima_df
    .select([pl.col("id_municipio").alias("id_municipio").cast(pl.Int32),
             pl.col("sigla_uf").alias("sigla_uf"),
             ]).unique().sort(["id_municipio", "sigla_uf"]))
# join with municipios to get municipio name
# join with uf to get uf name and regiao
dim_local_clima = (dim_local_clima
    .join(municipios_df.select([pl.col("MUNICÍPIO - IBGE"), pl.col('CÓDIGO DO MUNICÍPIO - IBGE')]), left_on="id_municipio", right_on="CÓDIGO DO MUNICÍPIO - IBGE", how="left")
    .join(uf_df.select([pl.col("sigla"), pl.col("nome"), pl.col("regiao")]), left_on="sigla_uf", right_on="sigla", how="left"))

dim_local_clima = dim_local_clima.select([
    pl.col("id_municipio"),
    pl.col("MUNICÍPIO - IBGE").alias("nome_municipio"),
    pl.col("sigla_uf"),
    pl.col("nome").alias("nome_uf"),
    pl.col("regiao").alias("regiao_uf")
]).sort(["id_municipio", "sigla_uf"]).unique().with_row_index("id_local")
dim_local_clima

# %%
# link dim_local_clima to dim_local_queimada on id_municipio and sigla_uf to get id_local_clima

print("Before join:", dim_local_queimada.shape)
dim_local_queimada = dim_local_queimada.join(
    dim_local_clima.select([pl.col("id_local").alias("id_local_clima"), pl.col("id_municipio"), pl.col("sigla_uf")]),
    on=["id_municipio", "sigla_uf"],
    how="left"
    )
print("After join:", dim_local_queimada.shape)
dim_local_queimada

# %%
# create fct_clima
print("Starting the join")
fct_clima = (clima_df.lazy().join(
    dim_data.lazy().select([pl.col("date_time_iso"), pl.col('id_data')]),
    left_on="data_hora", right_on="date_time_iso", how="left"
).with_columns([pl.col('id_municipio').cast(pl.Int32)])
# join by id_municipio, sigla_uf to get id_local_clima
.join(
    dim_local_clima.lazy().select([pl.col("id_local"), pl.col("id_municipio"), pl.col("sigla_uf")]),
    left_on=["id_municipio", "sigla_uf"],
    right_on=["id_municipio", "sigla_uf"],
    how="left"
)
# join dim_horarios_clima to get id_horario
.join(
    dim_horarios_clima.lazy().select([pl.col("id_horario"), pl.col("hora")]),
    left_on=[pl.col("data_hora").dt.hour()],
    right_on=["hora"],
    how="left"
)
.select([
    pl.col("id_data"),
    pl.col("id_local"),
    pl.col("id_horario"),
    pl.col('temperatura'),
    pl.col('umidade_relativa'),
    pl.col('vento_velocidade'),
    pl.col('vento_direcao'),
    pl.col('co_ppb'),
    pl.col('no2_ppb'),
    pl.col('o3_ppb'),
    pl.col('pm25_ugm3'),
    pl.col('so2_ugm3'),
    pl.col('precipitacao_dia')
])).unique().collect(streaming=True)
print("Finshished joining :)")
print(len(fct_clima))
print(fct_clima.head())

# %%
# load to parquet
dim_horarios_clima.write_parquet("data/dim_horarios_clima.pqt.zstd", compression="zstd")
dim_local_clima.write_parquet("data/dim_local_clima.pqt.zstd", compression="zstd")
dim_data.write_parquet("data/dim_data.pqt.zstd", compression="zstd")
fct_clima.write_parquet("data/fct_clima.pqt.zstd", compression="zstd")

dim_horarios_full.write_parquet("data/dim_horarios_queimada.pqt.zstd", compression="zstd")
dim_local_queimada.write_parquet("data/dim_local_queimada.pqt.zstd", compression="zstd")


