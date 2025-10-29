ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.17"

lazy val root = (project in file("."))
  .settings(
    name := "CinzasDoBrazil"
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.0.1"