ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-sql" % "3.5.0",
//  "org.apache.spark" %% "spark-hive" % "3.5.0",
  "org.postgresql" % "postgresql" % "42.3.0",
  "com.github.jnr" % "jnr-unixsocket" % "0.38.16",
  "org.xerial" % "sqlite-jdbc" % "3.36.0.3"
)

lazy val root = (project in file("."))
  .settings(
    name := "CardinalityEstimatorTest"
  )
