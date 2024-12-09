package experiments

import org.apache.spark.sql.SparkSession
import org.apache.commons.math3.stat.inference.TTest
import org.apache.spark.sql.internal.SQLConf

import scala.io.Source

object RuntimeComparison {

  private class ExecutionStatistics(val name: String) {

    var maxTrials: Int = 5
    var avgRuntime: Double = 0
    var runtimes: Array[Long] = Array()

    def setMaxTrials(v: Int): Unit = {
      this.maxTrials = v
    }

    def computeStats[T](f: => T): Unit = {
      val runs = (0 to this.maxTrials).map { _ =>
        val (_, t) = time { f }
        t
      }
      val times = runs.tail // ignore first few to remove warm-up time

      this.avgRuntime = times.sum.toDouble/times.length
      this.runtimes = times.toArray
    }

    def printStats(): Unit = {
      println(s"${this.name} stats")
      println(s"Average Runtime: ${this.avgRuntime}")
      println(s"All runtimes: ${this.runtimes.mkString("[", ",", "]")}")
    }

    def checkDiff(other: ExecutionStatistics): Boolean = {
      val tTest = new TTest()
      val pValue = tTest.tTest(this.runtimes.map(_.toDouble), other.runtimes.map(_.toDouble))

      println(s"P-value: $pValue")

      val alpha = 0.05
      pValue > alpha
    }
  }

  def setupSpark(master: String): SparkSession = {
    val spark = SparkSession.builder()
      .appName("FuzzTest")
      .master(master)
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  private def withSQLConf[T](pairs: (String, String)*)(f: => T): T = {
    val conf = SQLConf.get
    val (keys, values) = pairs.unzip
    val currentValues = keys.map { key =>
      if (conf.contains(key)) {
        Some(conf.getConfString(key))
      } else {
        None
      }
    }
    (keys, values).zipped.foreach { (k, v) =>
      conf.setConfString(k, v)
    }
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => conf.setConfString(key, value)
        case (key, None) => conf.unsetConf(key)
      }
    }
  }

  def withEnvA[T](f: => T): T = {
    // Sets up all the configurations for the Catalyst optimizer
    val optConfigs = Seq(
      (SQLConf.CBO_ENABLED.key, "true"),
      (SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key, "true")
    )
    withSQLConf(optConfigs: _*) {
      f
    }
  }

  def queryModA(q: String): String = {
    q
  }

  def withEnvB[T](f: => T): T = {
    val spark = SparkSession.getActiveSession match {
      case Some(session) => session
      case None => throw new RuntimeException("No active spark session")
    }

    val sparkOpt = spark.sessionState.optimizer
    val excludableRules = {
      val defaultRules = sparkOpt.defaultBatches.flatMap(_.rules.map(_.ruleName)).toSet
      val rules = defaultRules -- sparkOpt.nonExcludableRules.toSet
      rules
    }

    val excludedRules = excludableRules.mkString(",")
    val nonOptConfigs = Seq((SQLConf.OPTIMIZER_EXCLUDED_RULES.key, excludedRules))
    withSQLConf(nonOptConfigs: _*) {
      f
    }
  }

  def queryModB(q: String): String = {
    q
  }

  def readFileLines(filePath: String): List[String] = {
    val source = Source.fromFile(filePath)
    try {
      source.getLines().toList
    } finally {
      source.close()
    }
  }

  def time[T](f: => T): (T, Long) = {
    val st = System.nanoTime()
    val ret = f
    val et = System.nanoTime()
    (ret, et-st)
  }

  private class ComparisonManager[T, U](envA: (=> (U, Long)) => (U, Long),
                                        envB: (=> (U, Long)) => (U, Long),
                                        fA: => U,
                                        fB: => U,
                                        maxTrials: Int) {

    def runInterleaved(): ComparisonStats = {

      val statPairs = (0 until maxTrials).map {
        i =>
          val (_, timeA) = envA { time { fA } }
          val (_, timeB) = envB { time { fB } }

//          println(s"---- Trial $i ----")
//          println(s"Time A: $timeA")
//          println(s"Time B: $timeB")

          (timeA, timeB)
      }.toArray.tail


      new ComparisonStats(statPairs)
    }

  }

  class ComparisonStats(val statPairs: Array[(Long, Long)]) {


    def checkDiff(): Boolean = {
      val tTest = new TTest()
      val (a, b) = statPairs.unzip

      val pValue = tTest.tTest(a.map(_.toDouble), b.map(_.toDouble))

//      println(s"A mean: ${a.sum/a.length}")
//      println(s"B mean: ${b.sum/a.length}")
//      println(s"P-value: $pValue")

      val alpha = 0.01
      pValue <= alpha
    }
    def print(): Unit = {
      val sigDiff = this.checkDiff()
      val (a, b) = statPairs.unzip
//      println(s"Runtimes A ${a.mkString("[", ",", "]")}")
//      println(s"Runtimes B ${b.mkString("[", ",", "]")}")
      println(s"${if (sigDiff) "" else "no "}statistically significant difference in runtimes")
    }

  }

  def main(args: Array[String]): Unit = {
    val master = if(args.nonEmpty) args(0) else "local[*]"
    val queries_file = if(args.length == 2) args(1) else "test-queries.txt"
    val spark = setupSpark(master)
    val queries = readFileLines(queries_file)
    val maxTrials = 11


    queries.zipWithIndex.foreach {
      case (q, i) =>
        println(s"======= Query ${i} ==========")

        val compMan = new ComparisonManager[Long, Long] (
          withEnvA _,
          withEnvB _,
          {spark.sql(queryModA(q)).count()},
          {spark.sql(queryModB(q)).count()},
          maxTrials
        )

        val stats = compMan.runInterleaved()

        stats.print()
    }
  }



}
