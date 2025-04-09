package clustertests

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

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
      println(s"""${t.name}""")
    }

    val call_center = spark.table("tpcds.call_center").as("call_center")
    val catalog_page = spark.table("tpcds.catalog_page").as("catalog_page")
    val catalog_returns = spark.table("tpcds.catalog_returns").as("catalog_returns")
    val catalog_sales = spark.table("tpcds.catalog_sales").as("catalog_sales")
    val customer = spark.table("tpcds.customer").as("customer")
    val customer_address = spark.table("tpcds.customer_address").as("customer_address")
    val customer_demographics = spark.table("tpcds.customer_demographics").as("customer_demographics")
    val date_dim = spark.table("tpcds.date_dim").as("date_dim")
    val household_demographics = spark.table("tpcds.household_demographics").as("household_demographics")
    val income_band = spark.table("tpcds.income_band").as("income_band")
    val inventory = spark.table("tpcds.inventory").as("inventory")
    val item = spark.table("tpcds.item").as("item")
    val promotion = spark.table("tpcds.promotion").as("promotion")
    val reason = spark.table("tpcds.reason").as("reason")
    val ship_mode = spark.table("tpcds.ship_mode").as("ship_mode")
    val store = spark.table("tpcds.store").as("store")
    val store_returns = spark.table("tpcds.store_returns").as("store_returns")
    val store_sales = spark.table("tpcds.store_sales").as("store_sales")
    val time_dim = spark.table("tpcds.time_dim").as("time_dim")
    val warehouse = spark.table("tpcds.warehouse").as("warehouse")
    val web_page = spark.table("tpcds.web_page").as("web_page")
    val web_returns = spark.table("tpcds.web_returns").as("web_returns")
    val web_sales = spark.table("tpcds.web_sales")
    val web_site = spark.table("tpcds.web_site")

    // Select statements (you can run these with spark.sql if needed, but you're already using DataFrames)
    web_sales.join(web_site, col("web_sales.ws_sold_date_sk") === col("web_site.web_site_sk")).show(1)
//    spark.sql("SELECT * FROM call_center LIMIT 1")
//    spark.sql("SELECT * FROM catalog_page LIMIT 1")
//    spark.sql("SELECT * FROM catalog_returns LIMIT 1")
//    spark.sql("SELECT * FROM catalog_sales LIMIT 1")
//    spark.sql("SELECT * FROM customer LIMIT 1")
//    spark.sql("SELECT * FROM customer_address LIMIT 1")
//    spark.sql("SELECT * FROM customer_demographics LIMIT 1")
//    spark.sql("SELECT * FROM date_dim LIMIT 1")
//    spark.sql("SELECT * FROM household_demographics LIMIT 1")
//    spark.sql("SELECT * FROM income_band LIMIT 1")
//    spark.sql("SELECT * FROM inventory LIMIT 1")
//    spark.sql("SELECT * FROM item LIMIT 1")
//    spark.sql("SELECT * FROM promotion LIMIT 1")
//    spark.sql("SELECT * FROM reason LIMIT 1")
//    spark.sql("SELECT * FROM ship_mode LIMIT 1")
//    spark.sql("SELECT * FROM store LIMIT 1")
//    spark.sql("SELECT * FROM store_returns LIMIT 1")
//    spark.sql("SELECT * FROM store_sales LIMIT 1")
//    spark.sql("SELECT * FROM time_dim LIMIT 1")
//    spark.sql("SELECT * FROM warehouse LIMIT 1")
//    spark.sql("SELECT * FROM web_page LIMIT 1")
//    spark.sql("SELECT * FROM web_returns LIMIT 1")
//    spark.sql("SELECT * FROM web_sales LIMIT 1")
//    spark.sql("SELECT * FROM web_site LIMIT 1")

    // show(1) for each loaded DataFrame
//    call_center.show(1)
//    catalog_page.show(1)
//    catalog_returns.show(1)
//    catalog_sales.show(1)
//    customer.show(1)
//    customer_address.show(1)
//    customer_demographics.show(1)
//    date_dim.show(1)
//    household_demographics.show(1)
//    income_band.show(1)
//    inventory.show(1)
//    item.show(1)
//    promotion.show(1)
//    reason.show(1)
//    ship_mode.show(1)
//    store.show(1)
//    store_returns.show(1)
//    store_sales.show(1)
//    time_dim.show(1)
//    warehouse.show(1)
//    web_page.show(1)
//    web_returns.show(1)
//    web_sales.show(1)
//    web_site.show(1)

  }
}
