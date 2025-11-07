ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.17"

lazy val root = (project in file("."))
  .settings(
    name := "CinzasDoBrazil"
  )

val postgresqlVersion = "42.7.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "4.0.1",
  "org.postgresql" % "postgresql" % postgresqlVersion,
  "com.typesafe" % "config" % "1.4.3"
  )