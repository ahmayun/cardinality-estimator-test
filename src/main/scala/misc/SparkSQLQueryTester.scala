package misc

import org.apache.spark.sql.SparkSession

object SparkSQLQueryTester {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("PostgresSparkApp")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    println(spark.conf.get("spark.sql.catalogImplementation"))
    val q = """
      |select count(*) from person.person
      |""".stripMargin
    spark.sql(q).show(5)
  }
}
