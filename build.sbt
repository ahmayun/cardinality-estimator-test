ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.postgresql" % "postgresql" % "42.3.0"
)

lazy val root = (project in file("."))
  .settings(
    name := "CardinalityEstimatorTest"
  )
