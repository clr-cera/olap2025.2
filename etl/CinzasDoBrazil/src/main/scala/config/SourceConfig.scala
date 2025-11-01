package config


// Configurations for Extractors
case class SourceConfig(path : String, // Path to source
                        options : Map[String, String] // Map of options passed to read builder
                       )
