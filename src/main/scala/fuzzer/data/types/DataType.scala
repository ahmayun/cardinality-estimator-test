package fuzzer.data.types

import utils.Random

trait DataType {
  def name: String
}

object DataType {
  def generateRandom: DataType = {
    val l = Array(
      BooleanType,
      DateType,
      DecimalType,
      FloatType,
      IntegerType,
      LongType,
      StringType
    )

    l(Random.nextInt(l.length))
  }
}