package fuzzer.generation

import fuzzer.code.SourceCode
import fuzzer.graph.{DAGParser, DFOperator, Graph, Node}
import scala.util.Random
import scala.collection.mutable
import play.api.libs.json._
import utils.json.JsonReader

object Graph2Code {

  def constructDFOCall(spec: JsValue, node: Node[DFOperator], in1: String, in2: String): String = {
    val opName = node.value.name
    val opSpec = spec \ opName

    // Get operation type
    val opType = (opSpec \ "type").as[String]

    // Get parameters
    val parameters = (opSpec \ "parameters").as[JsObject]

    // Generate arguments based on parameters
    val args = generateArguments(parameters, opType, in2)

    // Construct the function call based on operation type
    opType match {
      case "source" => s"$opName(${args.mkString(", ")})"
      case "unary" => s"$in1.$opName(${args.mkString(", ")})"
      case "binary" => s"$in1.$opName(${args.mkString(", ")})"
      case "action" => s"$in1.$opName(${args.mkString(", ")})"
      case _ => s"$in1.$opName(${args.mkString(", ")})"
    }
  }

  def generateArguments(parameters: JsObject, opType: String, in2: String): List[String] = {
    val paramNames = parameters.keys.toList

    paramNames.flatMap { paramName =>
      val param = parameters \ paramName
      val paramType = getParamType((param \ "type").toOption.getOrElse(JsString("str")))
      val required = (param \ "required").as[Boolean]
      val hasDefault = (param \ "default").isDefined

      // For binary operations with DataFrame parameter, use in2
      if (paramName == "other" && paramType == "DataFrame" && opType == "binary") {
        Some(in2)
      } else if (required || (!hasDefault && Random.nextBoolean())) {
        // Generate a value for required parameters or randomly for optional ones
        Some(generateRandomValue(paramType, paramName))
      } else {
        None // Skip optional parameter
      }
    }
  }

  def getParamType(typeJson: JsValue): String = {
    typeJson match {
      case JsString(t) => t
      case JsArray(types) => types(Random.nextInt(types.length)).as[String]
      case _ => "str" // Default to string
    }
  }

  def generateRandomValue(paramType: String, paramName: String): String = {
    paramType match {
      case "int" => Random.nextInt(100).toString
      case "bool" => Random.nextBoolean().toString
      case "str" =>
        // Special handling for 'how' parameter in join
        if (paramName == "how") {
          val joinTypes = List("inner", "outer", "left", "right")
          s""""${joinTypes(Random.nextInt(joinTypes.size))}""""
        } else {
          s""""col_${Random.alphanumeric.take(5).mkString}""""
        }
      case "list" => s"""List("${Random.alphanumeric.take(5).mkString}")"""
      case "Column" => s"""col("${Random.alphanumeric.take(5).mkString}")"""
      case "DataFrame" => s"df_${Random.nextInt(5)}" // Generate a random DataFrame name
      case _ => s""""${Random.alphanumeric.take(8).mkString}""""
    }
  }

  def dag2Scala(spec: JsValue)(graph: Graph[DFOperator]): SourceCode = {
    val l = mutable.ListBuffer[String]()
    var count = 0
    val variablePrefix = "auto"


    graph.bfsBackwardsFromFinal { node =>
      if (node.parents.nonEmpty) {

        def decide(node: Node[DFOperator], i: String): String = {
          if (!node.hasAncestors)
            s"${constructDFOCall(spec, node, null, null)}"
          else
            s"${variablePrefix}${if (i == "L") count+1 else count+2}"
        }

        val left = decide(node.parents(0), "L")
        val right = if(node.parents.length > 1) decide(node.parents(1), "R") else ""
        l += s"val ${variablePrefix}${count} = ${constructDFOCall(spec, node, left, right)}"
        count += 1
      }
    }


    SourceCode(src=l.reverse.mkString("\n"), ast=null)
  }

  def main(args: Array[String]): Unit = {

    Random.setSeed("ahmad35".hashCode)

    val graph = DAGParser.parseYamlFile("dags/sample-dag-2.yaml", map => DFOperator.fromMap(map))
    val specScala = JsonReader.readJsonFile("specs/spark-scala.json")

    graph.computeReachabilityFromSources()

    val source = graph.generateCode(dag2Scala(specScala))

    println(source)

    graph.nodes.foreach { node =>
      println(s"${node.value} (${node.value.id}) is reachable from: ${node.reachableFromSources.map(_.value.id).mkString(",")}")
    }

  }
}
