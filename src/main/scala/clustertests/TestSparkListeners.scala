package clustertests

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}
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
//    val targetTables = gatherTargetTables(spark)
//    println("Printing target tables:")
//    targetTables.foreach{t =>
//      println(t)
//    }

    // ========= LISTENERS ========================
    class CpuTimeListener extends SparkListener {
      var cpuTime: Long = 0
      var peakMemory: Long = 0
//      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
//        val taskInfo = taskEnd.taskInfo
//        val taskMetrics = taskEnd.taskMetrics
//        val executorCpuTime = taskMetrics.executorCpuTime
//        cpuTime += executorCpuTime
//        println(s"Task ${taskInfo.taskId} completed with executor CPU time: $executorCpuTime ns")
//      }

      override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
        val stageInfo = stageCompleted.stageInfo
        val taskMetrics = stageInfo.taskMetrics
        val executorCpuTime = taskMetrics.executorCpuTime
        val peakMem = taskMetrics.peakExecutionMemory
        val status = stageInfo.failureReason match {
          case None => "Success"
          case Some(_) => "Failed"
        }
        cpuTime += executorCpuTime
        peakMemory = math.max(peakMem, peakMem)
        println(s"Stage ${stageInfo.stageId} [${status}] - CPU time: ${executorCpuTime}, Peak Mem: ${peakMem} (globalPeak: ${peakMemory})")
      }
    }
    val cpuListener = new CpuTimeListener()
    spark.sparkContext.addSparkListener(cpuListener)
    // ============================================

    //    val customer = spark.sql("select c_customer_sk from main.customer")
    //    val websales = spark.sql("select distinct ws_ship_customer_sk from main.web_sales sort by ws_ship_customer_sk")
    //    customer.show(5)
    //    println(s"Count: ${customer.count()}")
    //    websales.show(5)
    //    println(s"Count: ${websales.count()}")

    val q = """
      |select count(*) from main.customer inner join main.web_sales on ws_ship_customer_sk == c_customer_sk
      |""".stripMargin

    val df = spark.sql(q)
    df.show(5)

    println("Job details:")
    df.explain(true)
    println(s"Master: $master")
    println(s"Total CPU Time: ${cpuListener.cpuTime} ns")
    println(s"Peak Memory Usage: ${cpuListener.peakMemory} bytes")
    println("Holding job. Press <Enter> to end...")
    scala.io.StdIn.readLine()
    spark.sparkContext.removeSparkListener(cpuListener)

  }
}
