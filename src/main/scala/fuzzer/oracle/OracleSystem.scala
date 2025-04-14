package fuzzer.oracle

import java.io.{ByteArrayOutputStream, PrintStream}
import java.lang.reflect.InvocationTargetException
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox
import fuzzer.exceptions._
import fuzzer.templates.Harness
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import pipelines.local.ComparePlans.countTotalNodes
import scala.util.matching.Regex
import scala.collection.mutable

object OracleSystem {


  private def checkStateFromGlobal(): DataFrame = {
    val Some(df) = fuzzer.global.State.finalDF
    fuzzer.global.State.finalDF = None
    df
  }

  private def runWithSuppressOutput(source: String): (Throwable, String, String) = {
//    println(s"SOURCE:\n$source")
    val toolbox = currentMirror.mkToolBox()
    val outStream = new ByteArrayOutputStream()
    val errStream = new ByteArrayOutputStream()

    val psOut = new PrintStream(outStream)
    val psErr = new PrintStream(errStream)

    val throwable: Throwable = Console.withOut(psOut) {
      Console.withErr(psErr) {
        try {
          toolbox.eval(toolbox.parse(source))
          new Success("Success")
        } catch {
          case e: InvocationTargetException =>
            e.getCause
          case e: Exception =>
            new RuntimeException("Dynamic program invocation failed in OracleSystem.runWithSuppressOutput()", e)
        }
      }
    }

    psOut.close()
    psErr.close()

    (throwable, outStream.toString("UTF-8"), errStream.toString("UTF-8"))
  }

  private def createFullSourcesFromHarness(source: String): (String, String) = {
    val fullSourceOpt = Harness.embedCode(
      Harness.sparkProgramOptimizationsOn,
      source,
      Harness.insertionMark,
      "    " // indent 4 spaces
    )
    val fullSourceUnOpt = Harness.embedCode(
      Harness.sparkProgramOptimizationsOff,
      source,
      Harness.insertionMark,
      "    " // indent 4 spaces
    )

    (fullSourceOpt, fullSourceUnOpt)
  }

  private def oracleUDFDuplication(optDF: DataFrame, unOptDF: DataFrame): Throwable = {
    def countUDFs(plan: LogicalPlan): Int = {
      val planStr = plan.toString()
      val udfRegex: Regex = """UDF""".r  // Adjust this pattern to match your actual UDF name
      udfRegex.findAllIn(planStr).length
    }

    val optPlan = optDF.queryExecution.optimizedPlan
    val unOptPlan = unOptDF.queryExecution.optimizedPlan
    val optCount = countUDFs(optPlan)
    val unOptCount = countUDFs(unOptPlan)

    if (optCount != unOptCount)
      new MismatchException(
        s"""
           |UDF counts don't match Opt: $optCount, UnOpt: $unOptCount.
           |=== UnOptimized Plan ===
           |$unOptPlan
           |
           |=== Optimized Plan ===
           |$optPlan
           |""".stripMargin)
    else
      new Success(s"Success: Opt: $optCount - $unOptCount : UnOpt.")
  }

  private def compareRuns(optDF: DataFrame, unOptDF: DataFrame): Throwable = {
//    val mapOpt = mutable.Map[String, mutable.Map[Int, Int]]()
//    val mapUnOpt = mutable.Map[String, mutable.Map[Int, Int]]()

//    val countOpt = countTotalNodes(optDF.queryExecution.optimizedPlan, mapOpt)
//    val countUnOpt = countTotalNodes(unOptDF.queryExecution.optimizedPlan, mapUnOpt)

    oracleUDFDuplication(optDF, unOptDF)
  }


  def check(source: String): (Throwable, (Throwable, String), (Throwable, String)) = {
    val (fullSourceOpt, fullSourceUnOpt) = createFullSourcesFromHarness(source)
    val (resultOpt, stdOutOpt, stdErrOpt) = runWithSuppressOutput(fullSourceOpt)
    val optDF = resultOpt match {
      case _: Success => Some(checkStateFromGlobal())
      case _ => None
    }
    val (resultUnOpt, stdOutUnOpt, stdErrUnOpt) = runWithSuppressOutput(fullSourceUnOpt)
    val unOptDF = resultUnOpt match {
      case _: Success => Some(checkStateFromGlobal())
      case _ => None
    }

    val compare = (resultOpt, resultUnOpt) match {
      case (_: Success, _: Success) => compareRuns(optDF.get, unOptDF.get)
      case (a, b) if a.getClass == b.getClass => a
      case _ => new MismatchException("Execution result mismatch between optimized and unoptimized versions")
    }

    (compare, (resultOpt, fullSourceOpt), (resultUnOpt, fullSourceUnOpt))
  }

  def checkOneGo(source: String): (Throwable, (Throwable, String), (Throwable, String)) = {
    val (fullSourceOpt, fullSourceUnOpt) = createFullSourcesFromHarness(source)

    val combined = fullSourceOpt + fullSourceUnOpt
    val (result, stdOutOpt, stdErrOpt) = runWithSuppressOutput(combined)

    val compare = result match {
      case _: Success =>
        (fuzzer.global.State.optRunException, fuzzer.global.State.unOptRunException) match {
          case (Some(_: Success), Some(_: Success)) =>
            compareRuns(fuzzer.global.State.optDF.get, fuzzer.global.State.unOptDF.get)
          case (Some(a), Some(b)) if a.getClass == b.getClass =>
            a
          case _ =>
            new MismatchException("Execution result mismatch between optimized and unoptimized versions.")
        }
      case e: Throwable =>
        e
    }

    val optEx = fuzzer.global.State.optRunException.get
    val unOptEx = fuzzer.global.State.unOptRunException.get

    fuzzer.global.State.optDF = None
    fuzzer.global.State.unOptDF = None
    fuzzer.global.State.optRunException = None
    fuzzer.global.State.unOptRunException = None
    fuzzer.global.State.finalDF = None

    (compare, (optEx, fullSourceOpt), (unOptEx, fullSourceUnOpt))
  }
}


