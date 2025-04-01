ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-sql" % "3.5.0",
//  "org.apache.spark" %% "spark-hive" % "3.5.0",
  "org.postgresql" % "postgresql" % "42.3.0",
  "com.github.jnr" % "jnr-unixsocket" % "0.38.16",
  "org.xerial" % "sqlite-jdbc" % "3.36.0.3",
//  "ch.cern.sparkmeasure" %% "spark-measure" % "0.24",
  "com.github.javaparser" % "javaparser-core" % "3.25.5",
  "com.github.jsqlparser" % "jsqlparser" % "4.7",
  "org.scalameta" %% "scalameta" % "4.10.2",
  "com.typesafe.play" %% "play-json" % "2.9.4",
  "org.yaml" % "snakeyaml" % "2.2",

  //  "org.scalameta" %% "trees" % "4.7.1"
//  "org.scalameta" %% "semanticdb" % "4.7.1"

)

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.15.2"
dependencyOverrides += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2"


lazy val root = (project in file("."))
  .settings(
    name := "CardinalityEstimatorTest"
  )

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
  case _ => MergeStrategy.first
}
