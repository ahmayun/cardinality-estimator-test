


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import fuzzer.global.State.sparkOption
import sqlsmith.FuzzTests.withoutOptimized
import fuzzer.templates.ComplexObject
import fuzzer.exceptions._


object Optimized {

  def main(args: Array[String]): Unit = {
    val spark = sparkOption.get

    val preloadedUDF = udf((s: Any) => {
      val r = scala.util.Random.nextInt()
      ComplexObject(r,r)
    }).asNondeterministic()


    val auto5 = spark.read.parquet("tpcds-data/ship_mode").as("ship_mode")
    val auto0 = spark.read.parquet("tpcds-data/item").as("item")
    val auto6 = auto5.orderBy(col("ship_mode.sm_ship_mode_id"),preloadedUDF(col("ship_mode.sm_ship_mode_sk")))
    val auto8 = auto6.select(col("ship_mode.sm_ship_mode_sk"),col("ship_mode.sm_carrier"))
    val auto7 = auto6.limit(51)
    val auto1 = auto8.join(auto0, col("ship_mode.sm_carrier") === col("item.i_brand"), "inner")
    val auto3 = auto7.join(auto0, col("ship_mode.sm_ship_mode_sk") === col("item.i_manufact_id"), "inner")
    val auto4 = auto3.select(col("item.i_brand"),col("item.i_units"))
    val sink = auto4.join(auto1, col("ship_mode.sm_ship_mode_sk") === col("item.i_class_id"), "right")
    sink.explain(true)

    fuzzer.global.State.optDF = Some(sink)
  }
}

try {
   Optimized.main(Array())
} catch {
 case e =>
    fuzzer.global.State.optRunException = Some(e)
}

if (fuzzer.global.State.optRunException.isEmpty)
   fuzzer.global.State.optRunException = Some(new Success("Success"))
/*
fuzzer.exceptions.Success: Success
*/




import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import fuzzer.global.State.sparkOption
import sqlsmith.FuzzTests.withoutOptimized
import fuzzer.templates.ComplexObject
import fuzzer.exceptions._


object UnOptimized {

  def main(args: Array[String]): Unit = {
    val spark = sparkOption.get


    val preloadedUDF = udf((s: Any) => {
      val r = scala.util.Random.nextInt()
      ComplexObject(r,r)
    }).asNondeterministic()


    val sparkOpt = spark.sessionState.optimizer
    val excludableRules = {
      val defaultRules = sparkOpt.defaultBatches.flatMap(_.rules.map(_.ruleName)).toSet
      val rules = defaultRules -- sparkOpt.nonExcludableRules.toSet
      rules
    }
    val excludedRules = excludableRules.mkString(",")
    withoutOptimized(excludedRules) {
    val auto5 = spark.read.parquet("tpcds-data/ship_mode").as("ship_mode")
    val auto0 = spark.read.parquet("tpcds-data/item").as("item")
    val auto6 = auto5.orderBy(col("ship_mode.sm_ship_mode_id"),preloadedUDF(col("ship_mode.sm_ship_mode_sk")))
    val auto8 = auto6.select(col("ship_mode.sm_ship_mode_sk"),col("ship_mode.sm_carrier"))
    val auto7 = auto6.limit(51)
    val auto1 = auto8.join(auto0, col("ship_mode.sm_carrier") === col("item.i_brand"), "inner")
    val auto3 = auto7.join(auto0, col("ship_mode.sm_ship_mode_sk") === col("item.i_manufact_id"), "inner")
    val auto4 = auto3.select(col("item.i_brand"),col("item.i_units"))
    val sink = auto4.join(auto1, col("ship_mode.sm_ship_mode_sk") === col("item.i_class_id"), "right")
    sink.explain(true)

    fuzzer.global.State.unOptDF = Some(sink)
    }
  }
}


try {
   UnOptimized.main(Array())
} catch {
 case e =>
    fuzzer.global.State.unOptRunException = Some(e)
}

if (fuzzer.global.State.unOptRunException.isEmpty)
   fuzzer.global.State.unOptRunException = Some(new Success("Success"))

/*
fuzzer.exceptions.Success: Success
*/


/* ========== ORACLE RESULT ===================
fuzzer.exceptions.MismatchException: 
UDF counts don't match Opt: 1, UnOpt: 2.
=== UnOptimized Plan ===
Join RightOuter, (sm_ship_mode_sk#4301709 = i_class_id#4301724)
:- Project [i_brand#4301562, i_units#4301572]
:  +- Join Inner, (sm_ship_mode_sk#4301542 = i_manufact_id#4301567)
:     :- GlobalLimit 51
:     :  +- LocalLimit 51
:     :     +- Project [sm_ship_mode_sk#4301542, sm_ship_mode_id#4301543, sm_type#4301544, sm_code#4301545, sm_carrier#4301546, sm_contract#4301547]
:     :        +- Sort [sm_ship_mode_id#4301543 ASC NULLS FIRST, _nondeterministic#4301599 ASC NULLS FIRST], true
:     :           +- Project [sm_ship_mode_sk#4301542, sm_ship_mode_id#4301543, sm_type#4301544, sm_code#4301545, sm_carrier#4301546, sm_contract#4301547, UDF(sm_ship_mode_sk#4301542) AS _nondeterministic#4301599]
:     :              +- Relation [sm_ship_mode_sk#4301542,sm_ship_mode_id#4301543,sm_type#4301544,sm_code#4301545,sm_carrier#4301546,sm_contract#4301547] parquet
:     +- Relation [i_item_sk#4301554,i_item_id#4301555,i_rec_start_date#4301556,i_rec_end_date#4301557,i_item_desc#4301558,i_current_price#4301559,i_wholesale_cost#4301560,i_brand_id#4301561,i_brand#4301562,i_class_id#4301563,i_class#4301564,i_category_id#4301565,i_category#4301566,i_manufact_id#4301567,i_manufact#4301568,i_size#4301569,i_formulation#4301570,i_color#4301571,i_units#4301572,i_container#4301573,i_manager_id#4301574,i_product_name#4301575] parquet
+- Join Inner, (sm_carrier#4301713 = i_brand#4301723)
   :- Project [sm_ship_mode_sk#4301709, sm_carrier#4301713]
   :  +- Project [sm_ship_mode_sk#4301709, sm_ship_mode_id#4301710, sm_type#4301711, sm_code#4301712, sm_carrier#4301713, sm_contract#4301714]
   :     +- Sort [sm_ship_mode_id#4301710 ASC NULLS FIRST, _nondeterministic#4301599 ASC NULLS FIRST], true
   :        +- Project [sm_ship_mode_sk#4301709, sm_ship_mode_id#4301710, sm_type#4301711, sm_code#4301712, sm_carrier#4301713, sm_contract#4301714, UDF(sm_ship_mode_sk#4301709) AS _nondeterministic#4301599]
   :           +- Relation [sm_ship_mode_sk#4301709,sm_ship_mode_id#4301710,sm_type#4301711,sm_code#4301712,sm_carrier#4301713,sm_contract#4301714] parquet
   +- Relation [i_item_sk#4301715,i_item_id#4301716,i_rec_start_date#4301717,i_rec_end_date#4301718,i_item_desc#4301719,i_current_price#4301720,i_wholesale_cost#4301721,i_brand_id#4301722,i_brand#4301723,i_class_id#4301724,i_class#4301725,i_category_id#4301726,i_category#4301727,i_manufact_id#4301728,i_manufact#4301729,i_size#4301730,i_formulation#4301731,i_color#4301732,i_units#4301733,i_container#4301734,i_manager_id#4301735,i_product_name#4301736] parquet


=== Optimized Plan ===
Join RightOuter, (sm_ship_mode_sk#4301458 = i_class_id#4301473)
:- Project [i_brand#4301311, i_units#4301321]
:  +- Join Inner, (sm_ship_mode_sk#4301291 = i_manufact_id#4301316)
:     :- Filter isnotnull(sm_ship_mode_sk#4301291)
:     :  +- GlobalLimit 51
:     :     +- LocalLimit 51
:     :        +- Project [sm_ship_mode_sk#4301291]
:     :           +- Sort [sm_ship_mode_id#4301292 ASC NULLS FIRST, _nondeterministic#4301348 ASC NULLS FIRST], true
:     :              +- Project [sm_ship_mode_sk#4301291, sm_ship_mode_id#4301292, UDF(sm_ship_mode_sk#4301291) AS _nondeterministic#4301348]
:     :                 +- Relation [sm_ship_mode_sk#4301291,sm_ship_mode_id#4301292,sm_type#4301293,sm_code#4301294,sm_carrier#4301295,sm_contract#4301296] parquet
:     +- Project [i_brand#4301311, i_manufact_id#4301316, i_units#4301321]
:        +- Filter isnotnull(i_manufact_id#4301316)
:           +- Relation [i_item_sk#4301303,i_item_id#4301304,i_rec_start_date#4301305,i_rec_end_date#4301306,i_item_desc#4301307,i_current_price#4301308,i_wholesale_cost#4301309,i_brand_id#4301310,i_brand#4301311,i_class_id#4301312,i_class#4301313,i_category_id#4301314,i_category#4301315,i_manufact_id#4301316,i_manufact#4301317,i_size#4301318,i_formulation#4301319,i_color#4301320,i_units#4301321,i_container#4301322,i_manager_id#4301323,i_product_name#4301324] parquet
+- Join Inner, (sm_carrier#4301462 = i_brand#4301472)
   :- Project [sm_ship_mode_sk#4301458, sm_carrier#4301462]
   :  +- Filter isnotnull(sm_carrier#4301462)
   :     +- Relation [sm_ship_mode_sk#4301458,sm_ship_mode_id#4301459,sm_type#4301460,sm_code#4301461,sm_carrier#4301462,sm_contract#4301463] parquet
   +- Filter isnotnull(i_brand#4301472)
      +- Relation [i_item_sk#4301464,i_item_id#4301465,i_rec_start_date#4301466,i_rec_end_date#4301467,i_item_desc#4301468,i_current_price#4301469,i_wholesale_cost#4301470,i_brand_id#4301471,i_brand#4301472,i_class_id#4301473,i_class#4301474,i_category_id#4301475,i_category#4301476,i_manufact_id#4301477,i_manufact#4301478,i_size#4301479,i_formulation#4301480,i_color#4301481,i_units#4301482,i_container#4301483,i_manager_id#4301484,i_product_name#4301485] parquet


fuzzer.oracle.OracleSystem$.oracleUDFDuplication(OracleSystem.scala:91)
fuzzer.oracle.OracleSystem$.compareRuns(OracleSystem.scala:104)
fuzzer.oracle.OracleSystem$.checkOneGo(OracleSystem.scala:140)
fuzzer.core.MainFuzzer$.$anonfun$main$6(MainFuzzer.scala:180)
scala.collection.immutable.Range.foreach$mVc$sp(Range.scala:190)
fuzzer.core.MainFuzzer$.$anonfun$main$5(MainFuzzer.scala:170)
scala.util.control.Breaks.breakable(Breaks.scala:77)
fuzzer.core.MainFuzzer$.$anonfun$main$3(MainFuzzer.scala:170)
fuzzer.core.MainFuzzer$.$anonfun$main$3$adapted(MainFuzzer.scala:155)
scala.collection.ArrayOps$.foreach$extension(ArrayOps.scala:1323)
fuzzer.core.MainFuzzer$.$anonfun$main$2(MainFuzzer.scala:155)
scala.util.control.Breaks.breakable(Breaks.scala:77)
fuzzer.core.MainFuzzer$.main(MainFuzzer.scala:155)
fuzzer.core.MainFuzzer.main(MainFuzzer.scala)
