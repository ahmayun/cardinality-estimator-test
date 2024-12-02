package udfs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F}

class UDFs(val spark: SparkSession) {
  import spark.implicits._


  /**
   * This function checks if all elements in a delimited string are distinct.
   * It takes a list of items as a single string and a delimiter to split the items.
   * Returns true if all items are unique, otherwise returns false.
   * Example: isListDistinct("a,b,c,a", ',') returns false because "a" repeats.
   */
  def isListDistinct(list: String, delim: Char): Boolean = {
    if (list == null || list.isEmpty) return true

    val trimmedList = list.trim + delim
    val parts = scala.collection.mutable.Set[String]()
    val splitList = trimmedList.split(delim)

    for (part <- splitList) {
      val trimmedPart = part.trim
      if (parts.contains(trimmedPart)) {
        return false
      }
      parts.add(trimmedPart)
    }
    true
  }

  /**
   * Sudf_20b_GetManufactComplex
   * Remark by authors: Access multiple large fact tables
   * This function checks if a given item was sold in the year 2003 through either store sales or catalog sales.
   * If sold through both channels, it returns the item's manufacturer; otherwise, it returns "outdated item".
   * @param item The item identifier (integer).
   * @return The manufacturer of the item as a string or "outdated item".
   */
  def getManufactComplex(item: Int): String = {
    // Load Spark DataFrame references for the relevant tables
    val storeSalesHistory = spark.table("store_sales_history")
    val catalogSalesHistory = spark.table("catalog_sales_history")
    val dateDim = spark.table("date_dim")
    val itemTable = spark.table("item")

    // Check store sales count
    val cnt1 = storeSalesHistory
      .join(dateDim, storeSalesHistory("ss_sold_date_sk") === dateDim("d_date_sk"))
      .filter(storeSalesHistory("ss_item_sk") === item && dateDim("d_year") === 2003)
      .count()

    // Check catalog sales count
    val cnt2 = catalogSalesHistory
      .join(dateDim, catalogSalesHistory("cs_sold_date_sk") === dateDim("d_date_sk"))
      .filter(catalogSalesHistory("cs_item_sk") === item && dateDim("d_year") === 2003)
      .count()

    // Return result based on conditions
    if (cnt1 > 0 && cnt2 > 0) {
      val manufacturer = itemTable
        .filter(itemTable("i_item_sk") === item)
        .select("i_manufact")
        .as[String]
        .take(1)
        .headOption
      manufacturer.getOrElse("outdated item")
    } else {
      "outdated item"
    }
  }

}
