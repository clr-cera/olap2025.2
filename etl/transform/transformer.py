from typing import Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class Transformer:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        # Suppress WindowExec warning about no partition
        try:
            log4j = spark.sparkContext._jvm.org.apache.log4j
            log4j.LogManager.getLogger("org.apache.spark.sql.execution.window.WindowExec").setLevel(log4j.Level.ERROR)
        except Exception:
            pass
        self._dim_horarios_cache: Tuple[DataFrame, DataFrame] | None = None

    def preprocess_queimadas(self, queimadas_df: DataFrame) -> DataFrame:
        return (
            queimadas_df
            .filter(F.col("bioma").isNotNull())
            .withColumn("id_municipio", F.col("id_municipio").cast("int"))
            .withColumn("dias_sem_chuva", F.col("dias_sem_chuva").cast("long"))
            .withColumn(
                "dias_sem_chuva",
                F.when(F.col("dias_sem_chuva") == -999, F.lit(None)).otherwise(F.col("dias_sem_chuva"))
            )
            .withColumn(
                "risco_fogo",
                F.when(F.col("risco_fogo") == -999, F.lit(None)).otherwise(F.col("risco_fogo"))
            )
        )

    def preprocess_clima(self, clima_df: DataFrame) -> DataFrame:
        return (
            clima_df
            .filter(F.col("sigla_uf").isNotNull() & F.col("id_municipio").isNotNull())
            .withColumn("id_municipio", F.col("id_municipio").cast("int"))
        )

    def _get_season(self, month_col, day_col):
        return (F.when(
            ((month_col == 12) & (day_col >= 21)) | (month_col.isin([1, 2])) | ((month_col == 3) & (day_col < 20)), 1
        ).when(
            ((month_col == 3) & (day_col >= 20)) | (month_col.isin([4, 5])) | ((month_col == 6) & (day_col < 21)), 2
        ).when(
            ((month_col == 6) & (day_col >= 21)) | (month_col.isin([7, 8])) | ((month_col == 9) & (day_col < 22)), 3
        ).otherwise(4))

    def _build_dim_horarios_pair(self) -> Tuple[DataFrame, DataFrame]:
        if self._dim_horarios_cache is not None:
            return self._dim_horarios_cache

        hours = self.spark.range(0, 24).toDF("hora").select(F.col("hora").cast("int").alias("hora"))
        minutes = self.spark.range(0, 60).toDF("minuto").select(F.col("minuto").cast("int").alias("minuto"))

        window_minutes = Window.orderBy("hora", "minuto")
        dim_horarios = hours.crossJoin(minutes)
        dim_horarios = dim_horarios.withColumn(
            "id_horario",
            (F.row_number().over(window_minutes) - 1).cast("int")
        ).select("id_horario", "hora", "minuto")

        window_hours = Window.orderBy("hora")
        dim_horarios_clima = hours.withColumn(
            "id_horario",
            (F.row_number().over(window_hours) - 1).cast("int")
        ).select("id_horario", "hora")

        dim_horarios = dim_horarios.join(
            dim_horarios_clima.select(
                F.col("hora").alias("hora_link"),
                F.col("id_horario").alias("id_horario_clima")
            ),
            dim_horarios.hora == F.col("hora_link"),
            "left"
        ).drop("hora_link")

        self._dim_horarios_cache = (dim_horarios, dim_horarios_clima)
        return self._dim_horarios_cache

    def transform_dim_horarios(self) -> DataFrame:
        dim_horarios, _ = self._build_dim_horarios_pair()
        return dim_horarios

    def transform_dim_horarios_clima(self) -> DataFrame:
        _, dim_horarios_clima = self._build_dim_horarios_pair()
        return dim_horarios_clima

    def transform_dim_data(self, queimadas_df: DataFrame, clima_df: DataFrame) -> DataFrame:
        base_dates = (
            queimadas_df.select(F.col("data_hora").alias("date_time_iso"))
            .unionByName(clima_df.select(F.col("data_hora").alias("date_time_iso")))
            .dropna()
            .dropDuplicates(["date_time_iso"])
        )

        dim_data = base_dates.select(
            F.col("date_time_iso"),
            F.dayofmonth("date_time_iso").alias("dia"),
            F.month("date_time_iso").alias("mes"),
            F.year("date_time_iso").alias("ano"),
            F.dayofyear("date_time_iso").alias("dia_ano"),
            F.weekofyear("date_time_iso").alias("semana_ano"),
        )

        dim_data = dim_data.withColumn("semestre", F.when(F.col("mes") <= 6, 1).otherwise(2))
        dim_data = dim_data.withColumn("trimestre", F.floor((F.col("mes") - 1) / 3) + 1)

        dia_semana_raw = F.dayofweek("date_time_iso")
        dim_data = dim_data.withColumn("dia_semana", ((dia_semana_raw + 5) % 7 + 1).cast("int"))
        dim_data = dim_data.withColumn("is_weekend", F.col("dia_semana") >= 6)
        dim_data = dim_data.withColumn("estacao", self._get_season(F.col("mes"), F.col("dia")))

        window_dates = Window.orderBy("date_time_iso")
        dim_data = dim_data.withColumn(
            "id_data",
            (F.row_number().over(window_dates) - 1).cast("int")
        )

        return dim_data.select(
            "id_data",
            "date_time_iso",
            "dia",
            "mes",
            "ano",
            "semestre",
            "trimestre",
            "dia_semana",
            "dia_ano",
            "is_weekend",
            "semana_ano",
            "estacao",
        )

    def transform_dim_local_queimada(self, queimadas_df: DataFrame, municipios_df: DataFrame, uf_df: DataFrame) -> DataFrame:
        locs = queimadas_df.select(
            F.col("id_municipio"),
            F.col("sigla_uf"),
            F.col("bioma"),
            F.col("latitude"),
            F.col("longitude")
        ).distinct().withColumn("id_municipio", F.col("id_municipio").cast("int"))

        joined = locs.join(
            municipios_df,
            locs.id_municipio == municipios_df["CÓDIGO DO MUNICÍPIO - IBGE"],
            "left"
        ).join(
            uf_df,
            locs.sigla_uf == uf_df["sigla"],
            "left"
        )

        dim_local = joined.select(
            F.col("id_municipio").cast("int"),
            F.col("MUNICÍPIO - IBGE").alias("nome_municipio"),
            F.col("sigla_uf"),
            F.col("nome").alias("nome_uf"),
            F.col("regiao").alias("regiao_uf"),
            F.col("bioma"),
            F.col("latitude"),
            F.col("longitude")
        ).distinct()

        window_local = Window.orderBy("id_municipio", "sigla_uf", "latitude", "longitude", "bioma")
        dim_local = dim_local.withColumn(
            "id_local",
            (F.row_number().over(window_local) - 1).cast("int")
        )

        return dim_local

    def transform_dim_local_clima(self, clima_df: DataFrame, municipios_df: DataFrame, uf_df: DataFrame) -> DataFrame:
        locs = clima_df.select(
            F.col("id_municipio"),
            F.col("sigla_uf")
        ).distinct().withColumn("id_municipio", F.col("id_municipio").cast("int"))

        joined = locs.join(
            municipios_df,
            locs.id_municipio == municipios_df["CÓDIGO DO MUNICÍPIO - IBGE"],
            "left"
        ).join(
            uf_df,
            locs.sigla_uf == uf_df["sigla"],
            "left"
        )

        dim_local = joined.select(
            F.col("id_municipio").cast("int"),
            F.col("MUNICÍPIO - IBGE").alias("nome_municipio"),
            F.col("sigla_uf"),
            F.col("nome").alias("nome_uf"),
            F.col("regiao").alias("regiao_uf")
        ).distinct()

        window_local = Window.orderBy("id_municipio", "sigla_uf")
        dim_local = dim_local.withColumn(
            "id_local",
            (F.row_number().over(window_local) - 1).cast("int")
        )

        return dim_local

    def link_dim_local_dimensions(self, dim_local_queimada: DataFrame, dim_local_clima: DataFrame) -> DataFrame:
        clima_lookup = dim_local_clima.select(
            F.col("id_municipio"),
            F.col("sigla_uf"),
            F.col("id_local").alias("id_local_clima")
        )

        return dim_local_queimada.join(
            clima_lookup,
            on=["id_municipio", "sigla_uf"],
            how="left"
        )

    def transform_fct_queimadas(
        self,
        queimadas_df: DataFrame,
        dim_data: DataFrame,
        dim_local: DataFrame,
        dim_horarios: DataFrame
    ) -> DataFrame:
        q_df = queimadas_df.join(
            dim_data.select("date_time_iso", "id_data"),
            queimadas_df.data_hora == dim_data.date_time_iso,
            "left"
        )

        q_df = q_df.join(
            dim_local.select("id_local", "id_municipio", "sigla_uf", "latitude", "longitude", "bioma"),
            ["id_municipio", "sigla_uf", "latitude", "longitude", "bioma"],
            "left"
        )

        q_df = q_df.withColumn("hora_temp", F.hour("data_hora"))
        q_df = q_df.withColumn("minuto_temp", F.minute("data_hora"))
        q_df = q_df.join(
            dim_horarios,
            (q_df.hora_temp == dim_horarios.hora) & (q_df.minuto_temp == dim_horarios.minuto),
            "left"
        )

        fct = q_df.select(
            F.col("id_data"),
            F.col("id_local"),
            F.col("id_horario"),
            F.col("precipitacao"),
            F.col("risco_fogo"),
            F.col("potencia_radiativa_fogo"),
            F.col("dias_sem_chuva")
        ).dropDuplicates(["id_data", "id_local", "id_horario"])

        return fct

    def transform_fct_clima(
        self,
        clima_df: DataFrame,
        dim_data: DataFrame,
        dim_local: DataFrame,
        dim_horarios_clima: DataFrame
    ) -> DataFrame:
        c_df = clima_df.join(
            dim_data.select("date_time_iso", "id_data"),
            clima_df.data_hora == dim_data.date_time_iso,
            "left"
        )

        c_df = c_df.join(
            dim_local.select("id_local", "id_municipio", "sigla_uf"),
            ["id_municipio", "sigla_uf"],
            "left"
        )

        c_df = c_df.withColumn("hora_temp", F.hour("data_hora"))
        c_df = c_df.join(
            dim_horarios_clima,
            c_df.hora_temp == dim_horarios_clima.hora,
            "left"
        )

        fct = c_df.select(
            F.col("id_data"),
            F.col("id_local"),
            F.col("id_horario"),
            F.col("temperatura"),
            F.col("umidade_relativa"),
            F.col("vento_velocidade"),
            F.col("vento_direcao"),
            F.col("co_ppb"),
            F.col("no2_ppb"),
            F.col("o3_ppb"),
            F.col("pm25_ugm3"),
            F.col("so2_ugm3"),
            F.col("precipitacao_dia"),
        ).dropDuplicates(["id_data", "id_local", "id_horario"])

        return fct
