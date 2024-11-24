package clustertests

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession
import sqlsmith.FuzzTests.gatherTargetTables

object TestSparkListeners {
  def main(args: Array[String]): Unit = {

    val master = if(args.isEmpty) "local[*]" else args(0)

    val spark = SparkSession.builder()
      .appName("TestSparkListeners")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.statistics.size.autoUpdate.enabled", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .master(master)
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val targetTables = gatherTargetTables(spark)
    println("Printing target tables:")
    targetTables.foreach{t =>
      println(t)
    }

    // ========= LISTENERS ========================
    class CpuTimeListener extends SparkListener {
      var cpuTime: Long = 0
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        val taskInfo = taskEnd.taskInfo
        val taskMetrics = taskEnd.taskMetrics
        val executorCpuTime = taskMetrics.executorCpuTime
        cpuTime += executorCpuTime
//        println(s"Stage ${taskInfo.taskId} completed with executor CPU time: $executorCpuTime ns")
      }
    }
    val cpuListener = new CpuTimeListener()
    spark.sparkContext.addSparkListener(cpuListener)
    // ============================================

    val q = """
      |select count(*) from main.customer inner join main.web_sales on ws_ship_customer_sk == c_customer_sk
      |""".stripMargin

    spark.sql("select count(*) from main.customer")
    spark.sql("select count(*) from main.web_sales")
    spark.sql(q).show(5)
    println("Job details:")
    println(s"Master: $master")
    println(s"Total CPU Time: ${cpuListener.cpuTime} ns")

//    println("Holding job. Press <Enter> to end...")
//    scala.io.StdIn.readLine()

  }
}
