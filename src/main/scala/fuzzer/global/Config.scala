package fuzzer.global

object Config {

  val seed: Int = "ahmad35".hashCode
  val maxStringLength: Int = 5
  val intermediateVarPrefix = "auto"
  val finalVariableName = "sink"
  val probUDFInsert = 0.1 // The probability that a particular column expression for a unary op will be a UDF

  val maxListLength = 2
  val (randIntMin, randIntMax) = (-50, 50)
  val (randFloatMin, randFloatMax) = (-50.0, 50.0)
  val logicalOperatorSet = Set(">", "<", ">=", "<=")
}
