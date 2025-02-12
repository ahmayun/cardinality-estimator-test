package clustertests

import org.apache.spark.sql.SparkSession

object TestTPCDSEnvironment {

  def main(args: Array[String]): Unit = {
    val master = if(args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder()
      .appName("TestEnvironment")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master(master)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val targetTables = {
      spark.catalog.listDatabases().collect().flatMap(db => spark.catalog.listTables(db.name).collect())
    }.filter(_.database == "tpcds")

    targetTables.foreach { t =>
//      val df = spark.sql(s"select * from ${t.database}.${t.name}")
//      println(s"${t.name}: ${df.count()} rows")
      print(s""""${t.name}",""")
    }

    val df = spark.sql("select * from tpcds.store_returns where sr_return_quantity > 50")
    df.explain(true)

  }
}
