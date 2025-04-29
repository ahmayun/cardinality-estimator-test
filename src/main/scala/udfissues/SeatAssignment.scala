package udfissues

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import scala.collection.mutable
import scala.util.Random

// Simulate global seat pool and assignment registry
object SeatManager {
  val totalSeats = 21
  private val availableSeats: mutable.Set[Int] = mutable.Set((1 to totalSeats): _*)
  private val assignedSeats: mutable.Map[Long, Int] = mutable.Map()

  def assignRandomSeat(studentId: Long): Int = synchronized {

    if (availableSeats.isEmpty) {
      println(s"[ERROR] No available seats for student $studentId")
      return -1
    }

    val seat = Random.shuffle(availableSeats).head
    availableSeats.remove(seat)
    assignedSeats(studentId) = seat

    println(s"[INFO] Assigned seat $seat to student $studentId")
    seat
  }

  def printFinalAssignments(): Unit = {
    println("\n=== Final Seat Assignments ===")
    assignedSeats.toSeq.sortBy(_._1).foreach {
      case (studentId, seat) =>
        println(f"Student $studentId%3d -> Seat $seat%3d")
    }
    println(s"Total assigned: ${assignedSeats.size}, Remaining seats: ${availableSeats.size}")
  }
}

object SeatAssignment {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Seat Assignment UDF Bug")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val assignSeatUDF = udf((studentId: Long) => {
      SeatManager.assignRandomSeat(studentId)
    }).asNondeterministic() // Explicitly mark as non-deterministic

    val sparkOpt = spark.sessionState.optimizer
    val excludableRules = sparkOpt.defaultBatches.flatMap(_.rules.map(_.ruleName)).toSet -- sparkOpt.nonExcludableRules.toSet
    val excludedRules = excludableRules.mkString(",")
    SQLConf.get.setConfString(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, excludedRules)

    // Simulated student ID list
    spark.range(1, 21)
      .repartition(1)
      .write
      .mode("overwrite")
      .parquet("/tmp/student-ids")

    val students = spark.read.parquet("/tmp/student-ids")

    // This triggers the UDF - Spark may call it multiple times per row!
    val sorted = students.orderBy(assignSeatUDF(col("id")))
    sorted.show()

    // Print state to see duplicate assignments
    SeatManager.printFinalAssignments()
  }
}
