package fuzzer.graph

case class DFOperator(name: String, id: Int) {
  /*
    - inputs: List[DFOperator]
    - arguments: List[Primitive|UDF]
    -
   */
  override def toString: String = {
    name
  }
}

object DFOperator {
  def fromMap(map: Map[String, Any]): DFOperator = {
    DFOperator(
      name=map("op").asInstanceOf[String],
      id=map("id").asInstanceOf[Int]
    )
  }
}
