package sqlsmith

import org.apache.spark.sql.SparkSession

object TpcdsTablesLoader {

  def loadAll(spark: SparkSession, tpcdsDataPath: String): Unit = {

    spark.read.parquet(s"$tpcdsDataPath/call_center").createOrReplaceTempView("call_center")
    spark.read.parquet(s"$tpcdsDataPath/catalog_page").createOrReplaceTempView("catalog_page")
    spark.read.parquet(s"$tpcdsDataPath/catalog_returns").createOrReplaceTempView("catalog_returns")
    spark.read.parquet(s"$tpcdsDataPath/catalog_sales").createOrReplaceTempView("catalog_sales")
    spark.read.parquet(s"$tpcdsDataPath/customer").createOrReplaceTempView("customer")
    spark.read.parquet(s"$tpcdsDataPath/customer_address").createOrReplaceTempView("customer_address")
    spark.read.parquet(s"$tpcdsDataPath/customer_demographics").createOrReplaceTempView("customer_demographics")
    spark.read.parquet(s"$tpcdsDataPath/date_dim").createOrReplaceTempView("date_dim")
    spark.read.parquet(s"$tpcdsDataPath/household_demographics").createOrReplaceTempView("household_demographics")
    spark.read.parquet(s"$tpcdsDataPath/income_band").createOrReplaceTempView("income_band")
    spark.read.parquet(s"$tpcdsDataPath/inventory").createOrReplaceTempView("inventory")
    spark.read.parquet(s"$tpcdsDataPath/item").createOrReplaceTempView("item")
    spark.read.parquet(s"$tpcdsDataPath/promotion").createOrReplaceTempView("promotion")
    spark.read.parquet(s"$tpcdsDataPath/reason").createOrReplaceTempView("reason")
    spark.read.parquet(s"$tpcdsDataPath/ship_mode").createOrReplaceTempView("ship_mode")
    spark.read.parquet(s"$tpcdsDataPath/store").createOrReplaceTempView("store")
    spark.read.parquet(s"$tpcdsDataPath/store_returns").createOrReplaceTempView("store_returns")
    spark.read.parquet(s"$tpcdsDataPath/store_sales").createOrReplaceTempView("store_sales")
    spark.read.parquet(s"$tpcdsDataPath/time_dim").createOrReplaceTempView("time_dim")
    spark.read.parquet(s"$tpcdsDataPath/warehouse").createOrReplaceTempView("warehouse")
    spark.read.parquet(s"$tpcdsDataPath/web_page").createOrReplaceTempView("web_page")
    spark.read.parquet(s"$tpcdsDataPath/web_returns").createOrReplaceTempView("web_returns")
    spark.read.parquet(s"$tpcdsDataPath/web_sales").createOrReplaceTempView("web_sales")
    spark.read.parquet(s"$tpcdsDataPath/web_site").createOrReplaceTempView("web_site")

  }

}
