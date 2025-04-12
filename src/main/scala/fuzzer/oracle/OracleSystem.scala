package fuzzer.oracle

import java.io.{ByteArrayOutputStream, PrintStream}
import java.lang.reflect.InvocationTargetException
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox
import fuzzer.exceptions._
import fuzzer.templates.Harness
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.QueryExecution
import pipelines.local.ComparePlans.countTotalNodes

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

  private def compareRuns(optDF: DataFrame, unOptDF: DataFrame): Unit = {
    val mapOpt = mutable.Map[String, mutable.Map[Int, Int]]()
    val mapUnOpt = mutable.Map[String, mutable.Map[Int, Int]]()

    val countOpt = countTotalNodes(optDF.queryExecution.optimizedPlan, mapOpt)
    val countUnOpt = countTotalNodes(unOptDF.queryExecution.optimizedPlan, mapUnOpt)

    println(s"OPT $countOpt : $countUnOpt UNOPT")
  }

  def check(source: String): (Throwable, String, String) = {
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

    (resultOpt, resultUnOpt) match {
      case (_: Success, _: Success) => compareRuns(optDF.get, unOptDF.get)
      case _ =>
    }

    (resultOpt, fullSourceOpt, fullSourceUnOpt)
  }
}
