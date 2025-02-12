ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.spark" %% "spark-hive" % "3.5.0",
  "org.postgresql" % "postgresql" % "42.3.0",
  "com.github.jnr" % "jnr-unixsocket" % "0.38.16",
  "org.xerial" % "sqlite-jdbc" % "3.36.0.3",
  "ch.cern.sparkmeasure" %% "spark-measure" % "0.24",
  "com.github.javaparser" % "javaparser-core" % "3.25.5",
  "com.github.jsqlparser" % "jsqlparser" % "4.7"

)

lazy val root = (project in file("."))
  .settings(
    name := "CardinalityEstimatorTest"
  )

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
  case _ => MergeStrategy.first
}
