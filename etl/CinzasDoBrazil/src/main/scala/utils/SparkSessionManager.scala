package utils

import config.SparkConfig
import org.apache.spark.sql._

// Singleton for the SparkSession
object SparkSessionManager {
  @transient lazy val instance: SparkSession = {
    val builder = SparkSession.builder()
      .appName(SparkConfig.appName)
      .master(SparkConfig.master)
    val configuredBuilder = SparkConfig.options.foldLeft(builder) {
      case (b, (key, value)) => b.config(key, value)
    }
    configuredBuilder.getOrCreate()
  }
}
