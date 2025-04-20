package sqlsmith

import org.apache.spark.sql.SparkSession

object TpcdsTablesLoader {

  def loadAll(spark: SparkSession, tpcdsDataPath: String): Unit = {

    // 1. Make sure "main" database exists
    spark.sql("CREATE DATABASE IF NOT EXISTS main")

    // 2. List of all your table names
    val tableNames = Seq(
      "call_center",
      "catalog_page",
      "catalog_returns",
      "catalog_sales",
      "customer",
      "customer_address",
      "customer_demographics",
      "date_dim",
      "household_demographics",
      "income_band",
      "inventory",
      "item",
      "promotion",
      "reason",
      "ship_mode",
      "store",
      "store_returns",
      "store_sales",
      "time_dim",
      "warehouse",
      "web_page",
      "web_returns",
      "web_sales",
      "web_site"
    )

    // 3. Read each Parquet file, create temp view
    tableNames.foreach { tableName =>
      spark.read.parquet(s"$tpcdsDataPath/$tableName").createOrReplaceTempView(tableName)
    }

    // 4. Promote each temp view to a managed table inside "main"
    tableNames.foreach { tableName =>
      spark.sql(s"CREATE TABLE main.$tableName USING parquet AS SELECT * FROM $tableName")
    }

  }

}
