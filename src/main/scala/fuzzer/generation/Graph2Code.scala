package fuzzer.generation

import fuzzer.code.SourceCode
import fuzzer.data.tables.{ColumnMetadata, TableMetadata}
import fuzzer.data.types.{BooleanType, DataType, FloatType, IntegerType, StringType}
import fuzzer.exceptions.ImpossibleDFGException
import fuzzer.graph.{DAGParser, DFOperator, Graph, Node}

import utils.Random
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
      case "source" => constructSourceCall(node, spec, opName, opType, parameters, args)
      case "unary" => s"$in1.$opName(${args.mkString(", ")})"
      case "binary" => s"$in1.$opName(${args.mkString(", ")})"
      case "action" => s"$in1.$opName(${args.mkString(", ")})"
      case _ => s"$in1.$opName(${args.mkString(", ")})"
    }
  }

  private def constructSourceCall(node: Node[DFOperator], spec: JsValue, opName: String, opType: String, parameters: JsObject, args: List[String]): String = {
    opName match {
      case "spark.table" => s"""$opName("tpcds.${fuzzer.global.State.src2TableMap(node.id).identifier}")"""
      case _ => s"$opName(${args.mkString(", ")})"
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
    val sources = node.getReachableSources.toSeq
    assert(sources.nonEmpty, "Expected DAG sources to be non-empty")
    sources(Random.nextInt(sources.length))
  }

  def getAllColumns(node: Node[DFOperator]): Seq[(TableMetadata, ColumnMetadata)] = {
    val tablesColPairs = node.value.stateView.values.toSeq.flatMap { t =>
      t.columns.map(c => (t, c))
    }
    tablesColPairs
  }

  def pickRandomColumnFromReachableSources(node: Node[DFOperator]): (TableMetadata, ColumnMetadata) = {
    val tablesColPairs = getAllColumns(node).filter {
      case (_, col) =>
        col.metadata.get("gen-iteration") match {
          case None => true
          case Some(i) =>
            fuzzer.global.State.iteration.toString != i
        }
    }
    assert(tablesColPairs.nonEmpty, "Expected columnNames to be non-empty")
    val pick = tablesColPairs(Random.nextInt(tablesColPairs.length))
    pick
  }

  def renameTables(newValue: String, node: Node[DFOperator]): Unit = {
    val dfOp = node.value
//    println(s"====== RENAMING TABLE VIEW FOR ${node.id} (${node.value.name}) ===========")

    // Rename each table metadata entry in the stateView
    val renamedStateView: Map[String, TableMetadata] = dfOp.stateView.map {
      case (id, tableMeta) =>
        val renamed = tableMeta.copy() // get a deep copy
//        println(s"\t => At $id (${tableMeta.identifier} => $newValue)")
        renamed.setIdentifier(newValue) // modify as needed
        id -> renamed
    }

    // Update this node’s stateView with renamed copies
    dfOp.stateView = renamedStateView
  }

  def addColumn(value: String, node: Node[DFOperator]): Unit = {
    node.value.stateView = node.value.stateView + ("added" -> TableMetadata(
      _identifier = "",
      _columns = Seq(ColumnMetadata(name=value, dataType = DataType.generateRandom, metadata = Map("source" -> "runtime", "gen-iteration" -> fuzzer.global.State.iteration.toString))),
      _metadata = Map("source" -> "runtime", "gen-iteration" -> fuzzer.global.State.iteration.toString)
    ))
  }
  def updateSourceState(
                         node: Node[DFOperator],
                         param: JsLookupResult,
                         paramName: String,
                         paramType: String,
                         paramVal: String
                       ): Unit = {
    val effect = (param \ "state-effect").asOpt[String].get
    effect match {
      case "table-rename" => renameTables(paramVal, node)
      case "column-add" => addColumn(paramVal, node)
    }
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
      val gen = s"${Random.alphanumeric.take(fuzzer.global.Config.maxStringLength).mkString}"
      updateSourceState(node, param, paramName, paramType, gen)
      propagateState(node)
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

  def pickTwoColumns(stateView: Map[String, TableMetadata]): ((TableMetadata, ColumnMetadata), (TableMetadata, ColumnMetadata)) = {

    // Step 1: Flatten all columns and group by DataType -> Map[DataType, List[(TableMetadata, ColumnMetadata)]]
    val columnsByType: Map[DataType, Seq[(TableMetadata, ColumnMetadata)]] = stateView
      .values
      .toSeq
      .flatMap { table =>
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
    pickTwoColumns(node.value.stateView)
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
      case fuzzer.data.types.LongType => intExpr // TODO: Fix placeholder
      case fuzzer.data.types.DecimalType => intExpr // TODO: Fix placeholder
      case fuzzer.data.types.DateType => stringExpr // TODO: Fix placeholder
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
    val variablePrefix = "auto"


    graph.traverseTopological { node =>
      node.value.varName = s"$variablePrefix${node.id}"

      val call = node.getInDegree match {
        case 0 => constructDFOCall(spec, node, null, null)
        case 1 => constructDFOCall(spec, node, node.parents.head.value.varName, null)
        case 2 => constructDFOCall(spec, node, node.parents.head.value.varName, node.parents.last.value.varName)
      }
      val lhs = if(node.isSink) "" else s"val ${node.value.varName} = "
      l += s"$lhs$call"
    }


    SourceCode(src=l.mkString("\n"), ast=null)
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
        case (in, _) => throw new ImpossibleDFGException(s"Impossible DFG provided, a node has in-degree=$in")
      }
      assert(opOpt.isDefined, s"Couldn't find an operator in the provided spec that fits the node: $node")
      val Some(op) = opOpt
      new DFOperator(op, node.value.id)
    }
  }

  def initializeStateViews(graph: Graph[DFOperator]): Unit = {
    // Step 1: compute reachability from sources if not already done
    graph.computeReachabilityFromSources()

    // Step 2: for each node in the graph
    for (node <- graph.nodes) {
      val dfOp = node.value

      // Step 3: for each reachable source, create a copy of its state
      val stateCopies: Map[String, TableMetadata] = node.getReachableSources
        .map(source => source.id -> source.value.state.copy()) // assuming TableMetadata has copy()
        .toMap

      // Step 4: assign the stateView
      dfOp.stateView = stateCopies
    }
  }

  def propagateState(startNode: Node[DFOperator]): Unit = {
    val visited = mutable.Set[String]()
    val queue = mutable.Queue[Node[DFOperator]]()
    queue.enqueueAll(startNode.children)

    while (queue.nonEmpty) {
      val current = queue.dequeue()
      if (!visited.contains(current.id)) {
        visited += current.id

        val currentDFOp = current.value
        val startStateView = startNode.value.stateView

        // Only update keys that are also present in startNode.stateView
        val updatedView = currentDFOp.stateView.map {
          case (key, _) if startStateView.contains(key) =>
            key -> startStateView(key).copy()
          case other =>
            other
        }

        currentDFOp.stateView = updatedView

        // Enqueue children
        queue.enqueueAll(current.children)
      }
    }
  }

  def constructDFG(dag: Graph[DFOperator], apiSpec: JsValue, tables: List[TableMetadata]): Graph[DFOperator] = {
    val dfg = fillOperators(dag, apiSpec)
    dfg.computeReachabilityFromSources()
    val zipped = dfg.getSourceNodes.sortBy(_.value.id).zip(tables)
    zipped.foreach {
      case (node, table) =>
        node.value.state = table
    }

    fuzzer.global.State.src2TableMap = zipped.map {
      case (node, table) =>
        node.id -> table
    }.toMap

    initializeStateViews(dfg)
    dfg
  }

  def main(args: Array[String]): Unit = {

    val hardcodedTables: List[TableMetadata] = List(
      TableMetadata(
        _identifier = "users",
        _columns = Seq(
          ColumnMetadata("id", IntegerType, isNullable = false, isKey = true),
        ),
        _metadata = Map("source" -> "auth_system")
      ),
      TableMetadata(
        _identifier = "orders",
        _columns = Seq(
          ColumnMetadata("id", IntegerType, isNullable = false, isKey = true),
        ),
        _metadata = Map("source" -> "ecommerce")
      ),
      TableMetadata(
        _identifier = "products",
        _columns = Seq(
          ColumnMetadata("id", IntegerType, isNullable = false, isKey = true),
        ),
        _metadata = Map("source" -> "inventory")
      )
    )
    Random.setSeed("ahmad35".hashCode)

    val specScala = JsonReader.readJsonFile("specs/spark-scala.json")
    val dag = DAGParser.parseYamlFile("dags/sample-dag-4-bare.yaml", map => DFOperator.fromMap(map))
    val dfg = constructDFG(dag, specScala, hardcodedTables)
    val source = dfg.generateCode(dag2Scala(specScala))

    println(source)

  }

}
