package fuzzer.generation

import fuzzer.code.SourceCode
import fuzzer.data.tables.{ColumnMetadata, TableMetadata}
import fuzzer.data.types.{BooleanType, DataType, FloatType, IntegerType, StringType}
import fuzzer.graph.{DAGParser, DFOperator, Graph, Node}

import scala.util.Random
import scala.collection.mutable
import play.api.libs.json._
import utils.json.JsonReader

object Graph2Code {

  def constructDFOCall(spec: JsValue, node: Node[DFOperator], in1: String, in2: String): String = {
    val opName = node.value.name
    val opSpec = spec \ opName

    if (opSpec.isInstanceOf[JsUndefined]) {
      return opName
    }

    // Get operation type
    val opType = (opSpec \ "type").as[String]

    // Get parameters
    val parameters = (opSpec \ "parameters").as[JsObject]

    // Generate arguments based on parameters
    val args = generateArguments(node, parameters, opType, in2)

    // Construct the function call based on operation type
    opType match {
      case "source" => s"$opName(${args.mkString(", ")})"
      case "unary" => s"$in1.$opName(${args.mkString(", ")})"
      case "binary" => s"$in1.$opName(${args.mkString(", ")})"
      case "action" => s"$in1.$opName(${args.mkString(", ")})"
      case _ => s"$in1.$opName(${args.mkString(", ")})"
    }
  }

  def generateArguments(node: Node[DFOperator], parameters: JsObject, opType: String, in2: String): List[String] = {
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
        Some(generateRandomValue(node, param, paramType, paramName))
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

  def pickRandomReachableSource(node: Node[DFOperator]): Node[DFOperator] = {
    val sources = node.reachableFromSources.toSeq
    assert(sources.nonEmpty, "Expected DAG sources to be non-empty")
    sources(Random.nextInt(sources.length))
  }

  def pickRandomColumnFromReachableSources(node: Node[DFOperator]): (TableMetadata, ColumnMetadata) = {
    val randomSource = pickRandomReachableSource(node)
    val randomTable = randomSource.value.state
    val columns = randomTable.columns
    assert(columns.nonEmpty, "Expected columnNames to be non-empty")
    val randomColumn = columns(Random.nextInt(columns.length))
    (randomTable, randomColumn)
  }

  def generateOrPickString(
                            node: Node[DFOperator],
                            param: JsLookupResult,
                            paramName: String,
                            paramType: String
                          ): String = {

    val isStateAltering: Boolean = (param \ "state-altering").asOpt[Boolean].getOrElse(false)

    if (isStateAltering) {
      // Generate random string (e.g. for creating a new column)
      val gen = s"${Random.alphanumeric.take(5).mkString}"
      gen
    } else {
      // Pick a column name from reachable source nodes
      val (table, col) = pickRandomColumnFromReachableSources(node)
      constructFullColumnName(table, col)
    }
  }

  def constructFullColumnName(table: TableMetadata, col: ColumnMetadata): String = {
    val prefix = if(table.identifier != null && table.identifier.nonEmpty) s"${table.identifier}." else ""
    s"$prefix${col.name}"
  }

  def pickTwoColumns(sources: mutable.Set[Node[DFOperator]]): ((TableMetadata, ColumnMetadata), (TableMetadata, ColumnMetadata)) = {

    // Step 1: Flatten all columns and group by DataType -> Map[DataType, List[(TableMetadata, ColumnMetadata)]]
    val columnsByType: Map[DataType, Seq[(TableMetadata, ColumnMetadata)]] = sources
      .toSeq
      .flatMap { s =>
        val table = s.value.state
        table.columns.map(c => (c.dataType, (table, c)))
      }
      .groupBy(_._1)
      .view
      .mapValues(_.map(_._2))
      .toMap


    // Step 2: Filter to only those datatypes which have columns from at least 2 distinct tables
    val viableTypes: Seq[(DataType, Seq[(TableMetadata, ColumnMetadata)])] = columnsByType.toSeq
      .map { case (dt, cols) =>
        val tableGroups = cols.groupBy(_._1)  // group by TableMetadata
        (dt, tableGroups)
      }
      .filter { case (_, tableGroups) => tableGroups.size >= 2 }
      .map { case (dt, tableGroups) =>
        (dt, tableGroups.values.flatten.toSeq)
      }

    // Step 3: Randomly pick a datatype from viable options
    if (viableTypes.isEmpty) {
//      throw new RuntimeException("No two columns of the same type from different tables found")
      return null
    }

    val (_, candidates) = Random.shuffle(viableTypes).head

    // Step 4: Select two columns from different tables
    val byTable = candidates.groupBy(_._1).toSeq
    val shuffledPairs = Random.shuffle(byTable.combinations(2).toSeq)
    val chosenPair = shuffledPairs.head

    val col1 = Random.shuffle(chosenPair(0)._2).head
    val col2 = Random.shuffle(chosenPair(1)._2).head

    (col1, col2)
  }

  def pickMultiColumnsFromReachableSources(node: Node[DFOperator]): ((TableMetadata, ColumnMetadata), (TableMetadata, ColumnMetadata)) = {
    // pick one col each 2 datasets, must have same type

    val sources = node.reachableFromSources
    pickTwoColumns(sources)
  }

  def generateMultiColumnExpression(
                                     node: Node[DFOperator],
                                     param: JsLookupResult,
                                     paramName: String,
                                     paramType: String
                                   ): String = {

    val pair = pickMultiColumnsFromReachableSources(node)
    if (pair == null) {
      return null
    }
    val ((table1, col1), (table2, col2)) = pair
    val fullColName1 = constructFullColumnName(table1, col1)
    val fullColName2 = constructFullColumnName(table2, col2)
    val colExpr1 = s"""col("$fullColName1")"""
    val colExpr2 = s"""col("$fullColName2")"""
    val crossTableExpr = s"$colExpr1 === $colExpr2"
    crossTableExpr
  }

  def generateSingleColumnExpression(
                                      node: Node[DFOperator],
                                      param: JsLookupResult,
                                      paramName: String,
                                      paramType: String
                                    ): String = {

    val (table, col) = pickRandomColumnFromReachableSources(node)
    val fullColName = constructFullColumnName(table, col)
    val colExpr = s"""col("$fullColName")"""
    val intExpr = s"$colExpr > 5"
    val floatExpr = s"$colExpr > 5.0"
    val stringExpr = s"length($colExpr) > 5"
    val boolExpr = s"!$colExpr"

    col.dataType match {
      case fuzzer.data.types.IntegerType => intExpr
      case fuzzer.data.types.FloatType => floatExpr
      case fuzzer.data.types.StringType => stringExpr
      case fuzzer.data.types.BooleanType => boolExpr
    }
  }

  def generateColumnExpression(
                                node: Node[DFOperator],
                                param: JsLookupResult,
                                paramName: String,
                                paramType: String
                              ): String = {

    if (node.isBinary) {
      val expr = generateMultiColumnExpression(node, param, paramName, paramType)
      if (expr != null) {
        return expr
      }

    }
    generateSingleColumnExpression(node, param, paramName, paramType)
  }

  def generateRandomValue(node: Node[DFOperator], param: JsLookupResult, paramType: String, paramName: String): String = {
    // Try to get allowed values from the param JSON
    val allowedValues: Option[Seq[JsValue]] = (param \ "values").asOpt[Seq[JsValue]]

    // If allowed values are provided, pick one randomly
    allowedValues match {
      case Some(values) if values.nonEmpty =>
        val randomValue = values(Random.nextInt(values.length))
        randomValue match {
          case JsString(str) => s""""$str"""" // Add quotes for strings
          case other => other.toString       // Leave numbers, bools, etc. as-is
        }

    // Generate values if spec doesn't provide fixed options
      case _ =>
        paramType match {
          case "int" => Random.nextInt(100).toString
          case "bool" => Random.nextBoolean().toString
          case "str" => s""""${generateOrPickString(node, param, paramName, paramType)}""""
          case "list" => s"""List("${Random.alphanumeric.take(5).mkString}")"""
          case "Column" => generateColumnExpression(node, param, paramName, paramType)
          case "DataFrame" => s"df_${Random.nextInt(5)}"
          case _ => s""""${Random.alphanumeric.take(8).mkString}""""
        }
    }
  }

  def dag2Scala(spec: JsValue)(graph: Graph[DFOperator]): SourceCode = {
    val l = mutable.ListBuffer[String]()
    var count = 0
    val variablePrefix = "auto"


    graph.bfsBackwardsFromFinal { node =>
      println(node)
      if (node.parents.nonEmpty) {

        def decide(node: Node[DFOperator], i: String): String = {
          if (!node.hasAncestors)
            s"${constructDFOCall(spec, node, null, null)}"
          else
            s"${variablePrefix}${if (i == "L") count+1 else count+2}"
        }

        val left = decide(node.parents(0), "L")
        val right = if(node.parents.length > 1) decide(node.parents(1), "R") else ""
        l += s"val $variablePrefix$count = ${constructDFOCall(spec, node, left, right)}"
        count += 1
      }
    }


    SourceCode(src=l.reverse.mkString("\n"), ast=null)
  }

  def buildOpMap(spec: JsValue): Map[String, Seq[String]] = {
    spec.as[JsObject].fields.foldLeft(Map.empty[String, Seq[String]]) {
      case (acc, (name, definition)) =>
        val opType = (definition \ "type").as[String]
        acc.updatedWith(opType) {
          case Some(seq) => Some(seq :+ name)
          case None => Some(Seq(name))
        }
    }
  }

  def pickRandomSource(opMap: Map[String, Seq[String]]): Option[String] = {
    opMap.get("source") match {
      case Some(ops) =>
        val idx = Random.nextInt(ops.size)
        ops.lift(idx)
      case None => None
    }
  }

  def pickRandomUnaryOp(opMap: Map[String, Seq[String]]): Option[String] = {
    opMap.get("unary") match {
      case Some(ops) =>
        val idx = Random.nextInt(ops.size)
        ops.lift(idx)
      case None => None
    }
  }

  def pickRandomBinaryOp(opMap: Map[String, Seq[String]]): Option[String] = {
    opMap.get("binary") match {
      case Some(ops) =>
        val idx = Random.nextInt(ops.size)
        ops.lift(idx)
      case None => None
    }
  }

  def pickRandomAction(opMap: Map[String, Seq[String]]): Option[String] = {
    opMap.get("action") match {
      case Some(ops) =>
        val idx = Random.nextInt(ops.size)
        ops.lift(idx)
      case None => None
    }
  }

  private def fillOperators(graph: Graph[DFOperator], specScala: JsValue): Graph[DFOperator] = {
    val opMap = buildOpMap(specScala)

    graph.transformNodes { node =>
      val opOpt = (node.getInDegree, node.getOutDegree) match {
        case (_,0) => pickRandomAction(opMap)
        case (0,_) => pickRandomSource(opMap)
        case (1,_) => pickRandomUnaryOp(opMap)
        case (2,_) => pickRandomBinaryOp(opMap)
      }
      assert(opOpt.isDefined, s"Couldn't find an operator in the provided spec that fits the node: $node")
      val Some(op) = opOpt
      new DFOperator(op, node.value.id)
    }
  }

  def main(args: Array[String]): Unit = {
//    val hardcodedTables: List[TableMetadata] = List(
//      TableMetadata(
//        identifier = "users",
//        columns = Seq(
//          ColumnMetadata("id", IntegerType, isNullable = false, isKey = true),
//          ColumnMetadata("name", StringType),
//          ColumnMetadata("email", StringType)
//        ),
//        metadata = Map("source" -> "auth_system")
//      ),
//      TableMetadata(
//        identifier = "orders",
//        columns = Seq(
//          ColumnMetadata("order_id", IntegerType, isNullable = false, isKey = true),
//          ColumnMetadata("user_id", IntegerType),
//          ColumnMetadata("amount", FloatType),
//          ColumnMetadata("status", StringType)
//        ),
//        metadata = Map("source" -> "ecommerce")
//      ),
//      TableMetadata(
//        identifier = "products",
//        columns = Seq(
//          ColumnMetadata("product_id", IntegerType, isNullable = false, isKey = true),
//          ColumnMetadata("product_name", StringType),
//          ColumnMetadata("price", FloatType),
//          ColumnMetadata("available", BooleanType)
//        ),
//        metadata = Map("source" -> "inventory")
//      )
//    )

    val hardcodedTables: List[TableMetadata] = List(
      TableMetadata(
        identifier = "users",
        columns = Seq(
          ColumnMetadata("id", IntegerType, isNullable = false, isKey = true),
        ),
        metadata = Map("source" -> "auth_system")
      ),
      TableMetadata(
        identifier = "orders",
        columns = Seq(
          ColumnMetadata("id", IntegerType, isNullable = false, isKey = true),
        ),
        metadata = Map("source" -> "ecommerce")
      ),
      TableMetadata(
        identifier = "products",
        columns = Seq(
          ColumnMetadata("id", IntegerType, isNullable = false, isKey = true),
        ),
        metadata = Map("source" -> "inventory")
      )
    )
    Random.setSeed("ahmad35".hashCode)

    val specScala = JsonReader.readJsonFile("specs/spark-scala.json")
    val graph = DAGParser.parseYamlFile("dags/sample-dag-3.yaml", map => DFOperator.fromMap(map))
//    val graphRaw = DAGParser.parseYamlFile("dags/sample-dag-3.yaml", map => DFOperator.fromMap(map))
//    val graph = fillOperators(graphRaw, specScala)
    graph.traverseTopological(println)

    graph.computeReachabilityFromSources()
    graph.getSourceNodes.sortBy(_.value.id).zip(hardcodedTables).foreach {
      case (node, table) =>
        node.value.state = table
        println(s"${node.value.id} => ${table.identifier} ")
    }


    val source = graph.generateCode(dag2Scala(specScala))

    println(source)

  }

}
