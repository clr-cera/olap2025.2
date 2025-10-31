package config

object SparkConfig {
  val appName = "Cinzas do Brasil ETL"
  val master = "local[*]"
  val options : Map[String, String] = Map()
}
