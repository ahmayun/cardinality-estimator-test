package udfs

import org.apache.spark.sql.SparkSession

object Pairs extends Serializable {
//
//  val pairs = Array(
//    (
//      """
//        |SELECT to_upper(c_first_name) AS first_name_upper
//        |FROM main.customer;
//        |""".stripMargin,
//      """
//        |SELECT UPPER(c_first_name) AS first_name_upper
//        |FROM main.customer;
//        |""".stripMargin),
//    (
//      """
//        |SELECT calculate_age(c_birth_year) AS age
//        |FROM main.customer;
//        |""".stripMargin,
//      """
//        |SELECT YEAR(CURRENT_DATE) - c_birth_year AS age
//        |FROM main.customer;
//        |
//        |""".stripMargin),
//    (
//      """
//        |SELECT concat_names(c_first_name, c_last_name) AS full_name
//        |FROM main.customer;
//        |""".stripMargin,
//      """
//        |SELECT CONCAT(c_first_name, ' ', c_last_name) AS full_name
//        |FROM main.customer;
//        |""".stripMargin),
//    (
//      """
//        |SELECT d_date, is_weekend(d_date) AS is_weekend
//        |FROM main.date_dim;
//        |""".stripMargin,
//      """
//        |SELECT d_date,
//        |       CASE WHEN DAYOFWEEK(d_date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend
//        |FROM main.date_dim;
//        |""".stripMargin),
//    (
//      """
//        |SELECT calculate_discount(cs_list_price, cs_sales_price) AS discount_amount
//        |FROM main.catalog_sales;
//        |""".stripMargin,
//      """
//        |SELECT cs_list_price - cs_sales_price AS discount_amount
//        |FROM main.catalog_sales;
//        |""".stripMargin),
//    (
//      """
//        |SELECT extract_domain(c_email_address) AS email_domain
//        |FROM main.customer;
//        |""".stripMargin,
//      """
//        |SELECT SUBSTRING_INDEX(c_email_address, '@', -1) AS email_domain
//        |FROM main.customer;
//        |""".stripMargin),
//    (
//      """
//        |SELECT calculate_tax(ws_net_paid_inc_tax, ws_net_paid) AS tax_amount
//        |FROM main.web_sales;
//        |""".stripMargin,
//      """
//        |SELECT ws_net_paid_inc_tax - ws_net_paid AS tax_amount
//        |FROM main.web_sales;
//        |""".stripMargin),
//    (
//      """
//        |SELECT days_between(d_date, '2021-01-01') AS days_difference
//        |FROM main.date_dim;
//        |""".stripMargin,
//      """
//        |SELECT DATEDIFF(d_date, '2021-01-01') AS days_difference
//        |FROM main.date_dim;
//        |""".stripMargin),
//    (
//      """
//        |SELECT month_name(d_date) AS month_name
//        |FROM main.date_dim;
//        |""".stripMargin,
//      """
//        |SELECT DATE_FORMAT(d_date, 'MMMM') AS month_name
//        |FROM main.date_dim;
//        |""".stripMargin),
//    (
//      """
//        |SELECT string_reverse(c_last_name) AS reversed_last_name
//        |FROM main.customer;
//        |""".stripMargin,
//      """
//        |SELECT REVERSE(c_last_name) AS reversed_last_name
//        |FROM main.customer;
//        |""".stripMargin)
//  )

  // 1. UDF to convert a string to uppercase
  val toUpper: String => String = _.toUpperCase

//  // 2. UDF to calculate age from a birth year
//  val calculateAge: Int => Int = birthYear => java.time.Year.now.getValue - birthYear
//
//  // 3. UDF to concatenate first and last names
//  val concatNames: (String, String) => String = (firstName, lastName) => s"$firstName $lastName"
//
//  // 4. UDF to check if a date is on the weekend
//  val isWeekend: String => Boolean = date => {
//    val dayOfWeek = java.time.LocalDate.parse(date).getDayOfWeek.getValue
//    dayOfWeek == 6 || dayOfWeek == 7
//  }
//
//  // 5. UDF to calculate the discount amount
//  val calculateDiscount: (Double, Double) => Double = (listPrice, salesPrice) => listPrice - salesPrice
//
//  // 6. UDF to extract the domain from an email address
//  val extractDomain: String => String = email => email.split("@").last
//
//  // 7. UDF to calculate the tax amount
//  val calculateTax: (Double, Double) => Double = (netPaidIncTax, netPaid) => netPaidIncTax - netPaid
//
//  // 8. UDF to find days between two dates
//  val daysBetween: (String, String) => Int = (date1, date2) => {
//    val d1 = java.time.LocalDate.parse(date1)
//    val d2 = java.time.LocalDate.parse(date2)
//    java.time.temporal.ChronoUnit.DAYS.between(d1, d2).toInt
//  }
//
//  // 9. UDF to get the month name from a date
//  val monthName: String => String = date => {
//    java.time.LocalDate.parse(date).getMonth.getDisplayName(java.time.format.TextStyle.FULL, java.util.Locale.ENGLISH)
//  }
//
//  // 10. UDF to reverse a string
//  val stringReverse: String => String = _.reverse
}
