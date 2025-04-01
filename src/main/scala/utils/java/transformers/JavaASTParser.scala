//package utils.java.transformers
//import com.github.javaparser._
//import com.github.javaparser.ast._
//
//import java.nio.file.{Files, Paths}
//import java.nio.charset.StandardCharsets
//import java.io.PrintWriter
//import com.github.javaparser.ast.body._
//import com.github.javaparser.ast.stmt._
//import com.github.javaparser.ast.expr._
//import com.github.javaparser.ast.comments.LineComment
//import com.github.javaparser.ast.expr.AssignExpr.Operator
//import com.github.javaparser.ast.expr.SimpleName
//import com.github.javaparser.ast.nodeTypes.NodeWithBody
//
//import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
//import scala.collection.mutable
//import scala.collection.mutable.ListBuffer
//import scala.util.Random
//import scala.jdk.OptionConverters._
//import play.api.libs.json._
//import scala.io.Source
//
//
//object JavaASTParser {
//   private def parseJavaFile(filePath: String): CompilationUnit = {
//    val javaCode = new String(Files.readAllBytes(Paths.get(filePath)), StandardCharsets.UTF_8)
//    val parsed = new JavaParser().parse(javaCode)
//    parsed.getResult.orElseThrow(() => new RuntimeException("Failed to parse Java code"))
//  }
//
//  private def parseJavaStmt(stmt: String): Statement = {
//    try {
//      val parsed = StaticJavaParser.parseBlock(s"{$stmt}")
//      parsed
//    } catch {
//      case e: Throwable =>
//        sys.error(
//          s"""
//             |Failed to parse: $stmt
//             |${e.toString}
//             |""".stripMargin)
//    }
//
//  }
////  private def traverseAST(node: Node): Unit = {
////    node match {
////      case method: MethodDeclaration =>
////        println(s"Function Name: ${method.getNameAsString}")
////        println(s"Return Type: ${method.getType}")
////        println(s"Parameters: ${method.getParameters}")
////
////      case _ => // Continue traversal for child nodes
////    }
////
////    node.getChildNodes.forEach(traverseAST)
////  }
//
////  private def insertComments(
////                      node: Node,
////                      comments: List[String]
////                    ): (Node, Int) = {
////
////    if (comments.isEmpty) {
////      return (node, 0)
////    }
////
////    node match {
////      case method: MethodDeclaration =>
////        println(s"Function Name: ${method.getNameAsString}")
////        println(s"Return Type: ${method.getType}")
////        println(s"Parameters: ${method.getParameters}")
////        node.setComment(new LineComment("yoyoyo"))
////
////      case branch: com.github.javaparser.ast.stmt.IfStmt =>
////        println(s"Branch: ${branch.setComment(new LineComment("yoyoyoyo"))}")
////      case _ => // Continue traversal for child nodes
////    }
////
////    var total = 0
////    node.getChildNodes.forEach { child =>
////      val (_, insertedLines) = insertComments(child, comments)
////      total += insertedLines
////    }
////
////    (node, total)
////  }
//
//  private def findInsertionCandidates(node: Node, f: Node => Boolean): mutable.ListBuffer[Node] = {
//    val l = mutable.ListBuffer[Node]()
//    node match {
//      case x if f(x) => l += x
//      case _ => // continue
//    }
//
//    node.getChildNodes.forEach { child =>
//       l ++= findInsertionCandidates(child, f)
//    }
//    l
//  }
//
//  private def insertCommentsIntoAST(
//                             ast: CompilationUnit,
//                             comments: List[String],
//                             maxInserts: Int,
//                             bugInfo: Map[String, String]
//                           ): (CompilationUnit, Int) = {
//    val bugLine = bugInfo("line").toInt
//    val nodeRefs = findInsertionCandidates(ast.asInstanceOf[Node], {
//      case x: BlockStmt if x.getParentNode.get().isInstanceOf[ForStmt] => false // avoiding an occasional bug (I think in the library)
//      case _: MethodDeclaration => true
//      case _: Statement => true
//      case _ => false
//    })
//    val selectedLocs = Random.shuffle(nodeRefs).take(maxInserts)
//    val selectedComments = Random.shuffle(comments).take(maxInserts)
//    var additionalLines = 0
//    selectedLocs.zip(selectedComments).foreach {
//      case (loc, comment) if !loc.getComment.isPresent && loc.getRange.get().begin.line <= bugLine =>
//        additionalLines += 1
//        loc.setComment(new LineComment(comment))
//      case (loc, comment) =>
//        if(loc.getRange.get().begin.line <= bugLine) {
//          val totalLines = loc.getComment.get().getRange.get().getLineCount
//          additionalLines += 1 - totalLines
//        }
//        loc.setComment(new LineComment(comment))
//    }
//    (ast, bugLine+additionalLines)
//  }
//
//
//  private def writeASTToFile(ast: CompilationUnit, outputPath: String): Unit = {
//    val sourceCode = ast.toString
//    val writer = new PrintWriter(outputPath)
//    try {
//      writer.write(sourceCode)
//    } finally {
//      writer.close()
//    }
//  }
//
//  private def nodeFromLineNumber(node: Node, line: Int): Node = {
//    node match {
//      case n if n.hasRange && n.getRange.get().begin.line == line => n
//      case _ =>
//        var found: Node = null
//        node.getChildNodes.forEach { child =>
//          val result = nodeFromLineNumber(child, line)
//          found = if(found == null) result else found
//        }
//        found
//    }
//  }
//
//  private def printLineCount(deadStmt: Statement): Unit = {
//    println(
//      s"""
//         |$deadStmt
//         |Line count: ${deadStmt.getRange.get().getLineCount}
//         |""".stripMargin)
//  }
//
//  private def countLines(deadStmt: Statement): Int = {
//    val baseCount = deadStmt.getRange.get().getLineCount+2
//    var additional = 0
//    deadStmt.asBlockStmt().getStatements.forEach {
//      case stmt: IfStmt if stmt.getThenStmt.asBlockStmt().getStatements.isEmpty => additional += 1
//      case stmt: ForStmt if stmt.getBody.asBlockStmt().getStatements.isEmpty => additional += 1
//      case _ =>
//    }
//    baseCount + additional
//  }
//
//  private def insertDeadCode(ast: CompilationUnit, snippets: List[String], maxInserts: Int, bugInfo: Map[String, String]): (CompilationUnit, Int) = {
//    val bugLine = bugInfo("line").toInt
//    val deadStmts = snippets.map(parseJavaStmt)
//    val nodeRefs = findInsertionCandidates(ast, {
//      case _: Statement => true
//      case _: NodeWithBody[_] => true
//      case _ => false
//    })
//    val selectedLocs = Random.shuffle(nodeRefs).take(maxInserts)
//    val selectedDeadStmts = Random.shuffle(deadStmts).take(maxInserts)
//    var additionalLines = 0
//    selectedLocs.zip(selectedDeadStmts).foreach {
//      case (loc: BlockStmt, deadStmt) =>
//        additionalLines += (if(loc.getStatements.last.getRange.get().end.line+1 < bugLine) countLines(deadStmt) else 0)
//        loc.addStatement(deadStmt)
//        printLineCount(deadStmt)
//      case (loc: NodeWithBody[_], deadStmt) =>
//        additionalLines += (if(loc.getBody.asBlockStmt().getStatements.last.getRange.get().end.line+1 < bugLine) countLines(deadStmt) else 0)
//        loc.getBody.asBlockStmt().addStatement(deadStmt)
//        printLineCount(deadStmt)
//      case (loc: Statement, deadStmt) =>
//        val parentNode = loc.getParentNode.get
//        val locLine = loc.getRange.get().end.line
//        val index = parentNode match {
//          case block: BlockStmt =>
//            block.getStatements.indexOf(loc)
//          case parent =>
//            throw new IllegalStateException(s"loc: ${loc.getClass}\nparent: ${parent.getClass}")
//        }
//
//        parentNode match {
//          case block: BlockStmt =>
//            additionalLines += (if(locLine+1 < bugLine) countLines(deadStmt) else 0)
//            block.addStatement(index+1, deadStmt)
//        }
//      case _ =>
//    }
//    (ast, bugLine + additionalLines)
//  }
//
//  private def insertMisleadingVars(ast: CompilationUnit, varNames: List[String], maxInserts: Int, bugInfo: Map[String, String]): (CompilationUnit, Int) = {
//    val bugLine = bugInfo("line").toInt
//    val candidates = findInsertionCandidates(ast, {
//      case _: VariableDeclarator => true
//      case _ => false
//    })
//    Random.shuffle(candidates).take(maxInserts).zip(Random.shuffle(varNames).take(maxInserts)).foreach {
//      case (varDecl: VariableDeclarator, newName) =>
//        val oldName = varDecl.getName.getIdentifier
//        println(s"REPLACING $oldName => $newName")
//        findInsertionCandidates(ast, {
//          case _ : SimpleName => true
//          case _ => false
//        }).foreach {
//          case n: SimpleName if n.getIdentifier == oldName => n.setIdentifier(newName)
//          case _ =>
//        }
//    }
//    (ast, bugLine)
//  }
//
//  private def breakIntegerAssign(node: AssignExpr): Int = {
//    println(s"BREAKING $node")
//    // Get the variable name (left-hand side of assignment)
//    val target = node.getTarget
//    val varName = target.toString
//
//    // Create the first line: x = x;
//    val firstLine = new AssignExpr(
//      new NameExpr(varName),
//      new NameExpr(varName),
//      AssignExpr.Operator.ASSIGN
//    )
//
//    // Determine the binary operation for the second line
//    val secondLineExpr = node.getOperator match {
//      case AssignExpr.Operator.PLUS =>
//        new BinaryExpr(new NameExpr(varName), node.getValue, BinaryExpr.Operator.PLUS)
//      case AssignExpr.Operator.MINUS =>
//        new BinaryExpr(new NameExpr(varName), node.getValue, BinaryExpr.Operator.MINUS)
//      case AssignExpr.Operator.MULTIPLY =>
//        new BinaryExpr(new NameExpr(varName), node.getValue, BinaryExpr.Operator.MULTIPLY)
//      case AssignExpr.Operator.DIVIDE =>
//        new BinaryExpr(new NameExpr(varName), node.getValue, BinaryExpr.Operator.DIVIDE)
//      case AssignExpr.Operator.ASSIGN =>
//        node.getValue // Just the right-hand value
//      case _ =>
//        throw new UnsupportedOperationException(s"Unsupported operator: ${node.getOperator}")
//    }
//
//    // Create the second line: x = x + 1;
//    val secondLine = new AssignExpr(
//      new NameExpr(varName),
//      secondLineExpr,
//      AssignExpr.Operator.ASSIGN
//    )
//
//    // Replace the original node with the two new statements
//    val parent = node.getParentNode.orElse(null)
//    if (parent != null && parent.isInstanceOf[ExpressionStmt]) {
//      val block = parent.getParentNode.orElse(null)
//      if (block != null && block.isInstanceOf[com.github.javaparser.ast.stmt.BlockStmt]) {
//        val blockStmt = block.asInstanceOf[com.github.javaparser.ast.stmt.BlockStmt]
//        val stmts = blockStmt.getStatements
//        val index = stmts.indexOf(parent)
//
//        // Replace original statement with two new statements
//        stmts.set(index, new ExpressionStmt(firstLine))
//        stmts.add(index + 1, new ExpressionStmt(secondLine))
//        println(s"BROKE $node")
//      }
//    }
//
//    node.getRange.get().begin.line
//  }
//
//  private def breakMethodAssign(node: AssignExpr): Int = {
//      val target = node.getTarget
//      val varName = target.toString
//
//      // Create the first line: x = null;
//      val firstLine = new AssignExpr(new NameExpr(varName), new NullLiteralExpr(), AssignExpr.Operator.ASSIGN)
//
//      // Reuse the original assignment for the second line: x = func();
//      val secondLine = new AssignExpr(new NameExpr(varName), node.getValue, AssignExpr.Operator.ASSIGN)
//
//      // Get the parent statement and parent block
//      node.getParentNode.toScala match {
//        case Some(parentStmt: ExpressionStmt) =>
//          parentStmt.getParentNode.toScala match {
//            case Some(block: com.github.javaparser.ast.stmt.BlockStmt) =>
//              val stmts = block.getStatements
//              val index = stmts.indexOf(parentStmt)
//
//              if (index != -1) {
//                // Replace original statement with the two new ones
//                stmts.set(index, new ExpressionStmt(firstLine))
//                stmts.add(index + 1, new ExpressionStmt(secondLine))
//              }
//
//            case _ => println("Parent is not a block statement.")
//          }
//
//        case _ => println("Parent is not an expression statement.")
//      }
//
//      println(s"BROKE $node")
//      node.getRange.get().begin.line
//  }
//
//  private def breakExpression(expr: AssignExpr): Option[Int] = expr match {
//    case node if node.getValue.isInstanceOf[IntegerLiteralExpr] =>
//      Some(breakIntegerAssign(node))
//    case node if node.getValue.isInstanceOf[MethodCallExpr] =>
//      node.getOperator match {
//        case Operator.ASSIGN =>
//          Some(breakMethodAssign(node))
//        case _ =>
//          None
//      }
//  }
//
//
//  private def decomposeOperations(ast: CompilationUnit, maxInserts: Int, bugInfo: Map[String, String]): (CompilationUnit, Int) = {
//    val bugLine = bugInfo("line").toInt
//    val candidates = findInsertionCandidates(ast.asInstanceOf[Node], {
//      case _: AssignExpr => true
//      case _ => false
//    }).asInstanceOf[ListBuffer[AssignExpr]]
//
//    val lines = Random.shuffle(candidates).take(maxInserts).map(breakExpression)
//    println(s"Lines: $lines")
//    val additionalLines = lines.count {
//      case Some(i) => i < bugLine
//      case _ => false
//    }
//    (ast, bugLine + additionalLines)
//  }
//
//  case class Config(
//                     seed: String,
//                     comments: List[String],
//                     varNames: List[String],
//                     snippets: List[String],
//                     maxInsertsComments: Int,
//                     maxInsertsSnippets: Int,
//                     maxInsertsVars: Int,
//                     maxInsertsDecompose: Int
//                   )
//
//  def parseConfig(filePath: String): Config = {
//    // Read the JSON file
//    val jsonString = Source.fromFile(filePath).mkString
//    val json = Json.parse(jsonString)
//
//    // Extract the values from JSON
//    val comments = (json \ "mutations" \ "misleading_comments" \ "comments").as[List[String]]
//    val varNames = (json \ "mutations" \ "misleading_variables" \ "variables").as[List[String]]
//    val snippets = (json \ "mutations" \ "dead_code" \ "snippets").as[List[String]]
//
//    // Extract max_inserts values
//    val maxInsertsComments = (json \ "mutations" \ "misleading_comments" \ "max_inserts").as[Int]
//    val maxInsertsSnippets = (json \ "mutations" \ "dead_code" \ "max_inserts").as[Int]
//    val maxInsertsVars = (json \ "mutations" \ "misleading_variables" \ "max_inserts").as[Int]
//    val maxInsertsDecompose = (json \ "mutations" \ "decompose" \ "max_inserts").as[Int]
//    val seed = (json \ "configs" \ "seed").as[String]
//
//    Config(
//      seed = seed,
//      comments = comments,
//      varNames = varNames,
//      snippets = snippets,
//      maxInsertsComments = maxInsertsComments,
//      maxInsertsSnippets = maxInsertsSnippets,
//      maxInsertsVars = maxInsertsVars,
//      maxInsertsDecompose = maxInsertsDecompose
//    )
//  }
//
//  def parseBugInfo(filePath: String): Map[String, String] = {
//    val jsonString = Source.fromFile(filePath).mkString
//    val json = Json.parse(jsonString)
//
//    Map(
//      "reason" -> (json \ "reason").as[String],
//      "line" -> (json \ "line").as[Int].toString
//    )
//  }
//
//  def main(args: Array[String]): Unit = {
//
//    val dir = "src/main/scala/utils/java/transformers"
//    val inputFilePath = if (args.length > 0) args(0) else s"$dir/BuggyExample.txt"
//    val inputConfig = if (args.length > 1) args(0) else s"$dir/config.json"
//    val bugInfoFile = if (args.length > 2) args(0) else s"$dir/bug.json"
//    val outputFilePath = if (args.length > 3) args(1) else s"$dir/BuggyExampleModified.txt"
//    val config = parseConfig(inputConfig)
//    val bugInfo = parseBugInfo(bugInfoFile)
//
//    val seed = config.seed // buggy seed: -325456700 // Random.nextInt()
//    Random.setSeed(seed.hashCode)
//
//    println(s"""Buggy line in original file: ${bugInfo("line")}""")
//
//    // Converting to AST can change the code formatting so re-track the line
//    val _ast = parseJavaFile(inputFilePath)
//    val buggyNode = nodeFromLineNumber(_ast.asInstanceOf[Node], bugInfo("line").toInt)
//    val bugLineId = 567876545
//    println(s"Detected buggy Node: $buggyNode")
//    buggyNode.setComment(new LineComment(s"$bugLineId")) // mark the buggy line with a unique comment
//    println(s"Inserted identifier for buggy node: $bugLineId")
//
//    // Write to new file
//    val tmp = s"$dir/BuggyExampleIntermediate.txt"
//    writeASTToFile(_ast, tmp)
//
//    // Read the re-formatted file and search for buggy line's new position
//    val ast = parseJavaFile(tmp)
//    val newBuggyNode = ast.findAll(buggyNode.getClass).find {
//      case n if n.getComment.isPresent && n.getComment.get().getContent == bugLineId.toString => true
//      case _ => false
//    }
//    val newBuggyLine = newBuggyNode.get.getRange.get().end.line
//    println(s"Buggy node in intermediate file: $newBuggyNode")
//    println(s"Buggy node ends at line: $newBuggyLine")
//
//    // Insert comments
//    val (newAst, newLine) = insertCommentsIntoAST(ast, config.comments, config.maxInsertsComments, bugInfo.updated("line", newBuggyLine.toString))
//    println(s"Buggy line after mutation: $newLine")
//    val mutatedFile1 = s"$dir/BuggyExampleComments.txt"
//
//
//    writeASTToFile(newAst, mutatedFile1)
//    val ast2 = parseJavaFile(mutatedFile1)
//
//    // Insert dead code
//    val (newAst2, newLine2) = insertDeadCode(ast2, config.snippets, config.maxInsertsSnippets, bugInfo.updated("line", newLine.toString))
//    val mutatedFile2 = s"$dir/BuggyExampleDeadCode.txt"
//    println(s"Buggy line after mutation2: $newLine2")
//
//    writeASTToFile(newAst2, mutatedFile2)
//    val ast3 = parseJavaFile(mutatedFile2)
//
//    // Insert misleading vars
//    val (newAst3, newLine3) = insertMisleadingVars(ast3, config.varNames, config.maxInsertsVars, bugInfo.updated("line", newLine2.toString))
//    val mutatedFile3 = s"$dir/BuggyExampleMisleadingVars.txt"
//    println(s"Buggy line after mutation3: $newLine3")
//
//    writeASTToFile(newAst3, mutatedFile3)
//    val ast4 = parseJavaFile(mutatedFile3)
//
//    // Insert misleading vars
//    val (newAst4, newLine4) = decomposeOperations(ast4, config.maxInsertsDecompose, bugInfo.updated("line", newLine2.toString))
//    val mutatedFile4 = s"$dir/BuggyExampleDecomposedOps.txt"
//    println(s"Buggy line after mutation4: $newLine4")
//
//    writeASTToFile(newAst4, mutatedFile4)
//
//    writeASTToFile(newAst4, outputFilePath)
//
//    println(s"AST written back to source and saved at: $outputFilePath")
//    println(f"Seed: $seed")
//
//  }
//
//
//
//}
//
///*
//{
//    "configs": {
//      "seed": "seedlab-vt"
//    },
//    "mutations": {
//      "dead_code": {
//        "max_inserts": 4,
//        "snippets": [
//          "if (false) {\n    int tempVal = 999;\n    tempVal -= 1;\n    System.out.println(\"This block is never executed\");\n}",
//          "int placeholder = 123;\nplaceholder += 0;\nif (placeholder > 9999) {\n    System.out.println(\"Completely unused check\");\n}",
//          "for (int i = 0; i < 3; i++) {}\nboolean dummyCondition = true;\nif (dummyCondition && false) {\n    System.out.println(\"Logic that won't run\");\n}",
//          "int result = 42;\nresult += 0;\nif (!(result < 0)) {}\nSystem.out.println(\"# This line won't affect anything\");"
//        ]
//      },
//      "misleading_comments": {
//        "max_inserts": 4,
//        "comments": [
//          "// Ensures parallel universe coherence remains stable",
//          "// This line triggers advanced warp field calibration",
//          "// Optimizes quantum chromodynamics at runtime",
//          "// Secretly reconfigures matrix multiplication for negative time cycles"
//        ]
//      },
//      "misleading_variables": {
//        "max_inserts": 5,
//        "variables": [
//          "serializerDragon",
//          "classMorph",
//          "quantumMatcher",
//          "warpKey",
//          "enumGuardian"
//        ]
//      },
//      "decompose": {
//        "max_inserts": 1
//      }
//    }
//  }
//
//
//  {
//    "reason": "Misplaced return",
//    "line": 37
//}
// */
