package config

object SparkConfig {
  val appName = "Cinzas do Brasil ETL"
  val master = "local[*]"
  val options : Map[String, String] = Map("spark.executor.memory" -> "8g")
}
