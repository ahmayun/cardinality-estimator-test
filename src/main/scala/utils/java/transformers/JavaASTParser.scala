package utils.java.transformers
import com.github.javaparser._
import com.github.javaparser.ast._

import java.util.function.Predicate
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import java.io.PrintWriter
import com.github.javaparser.ast.body._
import com.github.javaparser.ast.stmt._
import com.github.javaparser.ast.comments.LineComment
import com.github.javaparser.ast.expr.SimpleName
import com.github.javaparser.ast.nodeTypes.NodeWithBody
import com.github.javaparser.ast.visitor.ModifierVisitor
import com.github.javaparser.resolution.declarations.ResolvedValueDeclaration

import java.util.Optional
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable
import scala.util.Random


object JavaASTParser {
   private def parseJavaFile(filePath: String): CompilationUnit = {
    val javaCode = new String(Files.readAllBytes(Paths.get(filePath)), StandardCharsets.UTF_8)
    val parsed = new JavaParser().parse(javaCode)
    parsed.getResult.orElseThrow(() => new RuntimeException("Failed to parse Java code"))
  }

  private def parseJavaStmt(stmt: String): Statement = {
    try {
      val parsed = StaticJavaParser.parseBlock(s"{$stmt}")
      parsed
    } catch {
      case e: Throwable =>
        sys.error(
          s"""
             |Failed to parse: $stmt
             |${e.toString}
             |""".stripMargin)
    }

  }
//  private def traverseAST(node: Node): Unit = {
//    node match {
//      case method: MethodDeclaration =>
//        println(s"Function Name: ${method.getNameAsString}")
//        println(s"Return Type: ${method.getType}")
//        println(s"Parameters: ${method.getParameters}")
//
//      case _ => // Continue traversal for child nodes
//    }
//
//    node.getChildNodes.forEach(traverseAST)
//  }

//  private def insertComments(
//                      node: Node,
//                      comments: List[String]
//                    ): (Node, Int) = {
//
//    if (comments.isEmpty) {
//      return (node, 0)
//    }
//
//    node match {
//      case method: MethodDeclaration =>
//        println(s"Function Name: ${method.getNameAsString}")
//        println(s"Return Type: ${method.getType}")
//        println(s"Parameters: ${method.getParameters}")
//        node.setComment(new LineComment("yoyoyo"))
//
//      case branch: com.github.javaparser.ast.stmt.IfStmt =>
//        println(s"Branch: ${branch.setComment(new LineComment("yoyoyoyo"))}")
//      case _ => // Continue traversal for child nodes
//    }
//
//    var total = 0
//    node.getChildNodes.forEach { child =>
//      val (_, insertedLines) = insertComments(child, comments)
//      total += insertedLines
//    }
//
//    (node, total)
//  }

  private def findInsertionCandidates(node: Node, f: Node => Boolean): mutable.MutableList[Node] = {
    val l = mutable.MutableList[Node]()
    node match {
      case x if f(x) => l += x
      case _ => // continue
    }

    node.getChildNodes.forEach { child =>
       l ++= findInsertionCandidates(child, f)
    }
    l
  }

  private def insertCommentsIntoAST(
                             ast: CompilationUnit,
                             comments: List[String],
                             maxInserts: Int,
                             bugInfo: Map[String, String]
                           ): (CompilationUnit, Int) = {
    val bugLine = bugInfo("line").toInt
    val nodeRefs = findInsertionCandidates(ast.asInstanceOf[Node], {
      case x: BlockStmt if x.getParentNode.get().isInstanceOf[ForStmt] => false // avoiding an occasional bug (I think in the library)
      case _: MethodDeclaration => true
      case _: Statement => true
      case _ => false
    })
    val selectedLocs = Random.shuffle(nodeRefs).take(maxInserts)
    val selectedComments = Random.shuffle(comments).take(maxInserts)
    var additionalLines = 0
    selectedLocs.zip(selectedComments).foreach {
      case (loc, comment) if !loc.getComment.isPresent && loc.getRange.get().begin.line <= bugLine =>
        additionalLines += 1
        loc.setComment(new LineComment(comment))
      case (loc, comment) =>
        if(loc.getRange.get().begin.line <= bugLine) {
          val totalLines = loc.getComment.get().getRange.get().getLineCount
          additionalLines += 1 - totalLines
        }
        loc.setComment(new LineComment(comment))
    }
    (ast, bugLine+additionalLines)
  }


  private def writeASTToFile(ast: CompilationUnit, outputPath: String): Unit = {
    val sourceCode = ast.toString
    val writer = new PrintWriter(outputPath)
    try {
      writer.write(sourceCode)
    } finally {
      writer.close()
    }
  }

  private def nodeFromLineNumber(node: Node, line: Int): Node = {
    node match {
      case n if n.hasRange && n.getRange.get().begin.line == line => n
      case _ =>
        var found: Node = null
        node.getChildNodes.forEach { child =>
          val result = nodeFromLineNumber(child, line)
          found = if(found == null) result else found
        }
        found
    }
  }

  private def printLineCount(deadStmt: Statement): Unit = {
    println(
      s"""
         |$deadStmt
         |Line count: ${deadStmt.getRange.get().getLineCount}
         |""".stripMargin)
  }

  private def countLines(deadStmt: Statement): Int = {
    val baseCount = deadStmt.getRange.get().getLineCount+2
    var additional = 0
    deadStmt.asBlockStmt().getStatements.forEach {
      case stmt: IfStmt if stmt.getThenStmt.asBlockStmt().getStatements.isEmpty => additional += 1
      case stmt: ForStmt if stmt.getBody.asBlockStmt().getStatements.isEmpty => additional += 1
      case _ =>
    }
    baseCount + additional
  }

  private def insertDeadCode(ast: CompilationUnit, snippets: List[String], maxInserts: Int, bugInfo: Map[String, String]): (CompilationUnit, Int) = {
    val bugLine = bugInfo("line").toInt
    val deadStmts = snippets.map(parseJavaStmt)
    val nodeRefs = findInsertionCandidates(ast, {
      case _: Statement => true
      case _: NodeWithBody[_] => true
      case _ => false
    })
    val selectedLocs = Random.shuffle(nodeRefs).take(maxInserts)
    val selectedDeadStmts = Random.shuffle(deadStmts).take(maxInserts)
    var additionalLines = 0
    selectedLocs.zip(selectedDeadStmts).foreach {
      case (loc: BlockStmt, deadStmt) =>
        additionalLines += (if(loc.getStatements.last.getRange.get().end.line+1 < bugLine) countLines(deadStmt) else 0)
        loc.addStatement(deadStmt)
        printLineCount(deadStmt)
      case (loc: NodeWithBody[_], deadStmt) =>
        additionalLines += (if(loc.getBody.asBlockStmt().getStatements.last.getRange.get().end.line+1 < bugLine) countLines(deadStmt) else 0)
        loc.getBody.asBlockStmt().addStatement(deadStmt)
        printLineCount(deadStmt)
      case (loc: Statement, deadStmt) =>
        val parentNode = loc.getParentNode.get
        val locLine = loc.getRange.get().end.line
        val index = parentNode match {
          case block: BlockStmt =>
            block.getStatements.indexOf(loc)
          case parent =>
            throw new IllegalStateException(s"loc: ${loc.getClass}\nparent: ${parent.getClass}")
        }

        parentNode match {
          case block: BlockStmt =>
            additionalLines += (if(locLine+1 < bugLine) countLines(deadStmt) else 0)
            block.addStatement(index+1, deadStmt)
        }
      case _ =>
    }
    (ast, bugLine + additionalLines)
  }

  private def insertMisleadingVars(ast: CompilationUnit, varNames: List[String], maxInserts: Int, bugInfo: Map[String, String]): (CompilationUnit, Int) = {
    val bugLine = bugInfo("line").toInt
    val candidates = findInsertionCandidates(ast, {
      case _: VariableDeclarator => true
      case _ => false
    })
    Random.shuffle(candidates).take(maxInserts).zip(Random.shuffle(varNames).take(maxInserts)).foreach {
      case (varDecl: VariableDeclarator, newName) =>
        val oldName = varDecl.getName.getIdentifier
        println(s"REPLACING $oldName => $newName")
        findInsertionCandidates(ast, {
          case _ : SimpleName => true
          case _ => false
        }).foreach {
          case n: SimpleName if n.getIdentifier == oldName => n.setIdentifier(newName)
          case _ =>
        }
    }
    (ast, bugLine)
  }


  def main(args: Array[String]): Unit = {

    val dir = "/home/ahmad/Documents/project/cardinality-estimator-test/src/main/scala/utils/java/transformers/"
    val inputFilePath = if (args.length > 0) args(0) else s"$dir/BuggyExample.txt"
    val outputFilePath = if (args.length > 1) args(1) else s"$dir/BuggyExampleModified.txt"
    val comments = List(
        "// Ensures parallel universe coherence remains stable",
        "// This line triggers advanced warp field calibration",
        "// Optimizes quantum chromodynamics at runtime",
        "// Secretly reconfigures matrix multiplication for negative time cycles"
    )
    val snippets = List(
      "if (false) {\n    int tempVal = 999;\n    tempVal -= 1;\n    System.out.println(\"This block is never executed\");\n}",
      "int placeholder = 123;\nplaceholder += 0;\nif (placeholder > 9999) {\n    System.out.println(\"Completely unused check\");\n}",
      "for (int i = 0; i < 3; i++) {}\nboolean dummyCondition = true;\nif (dummyCondition && false) {\n    System.out.println(\"Logic that won't run\");\n}",
      "int result = 42;\nresult += 0;\nif (!(result < 0)) {}\nSystem.out.println(\"# This line won't affect anything\");"
    )
    val varNames = List(
      "serializerDragon",
      "classMorph",
      "quantumMatcher",
      "warpKey",
      "enumGuardian"
    )

    val seed =  "seed-vt" // buggy seed: -325456700 // Random.nextInt()
    Random.setSeed(seed.hashCode)

    val max_inserts = comments.length
    val bugInfo = Map("reason" -> "misplaced return", "line" -> "47" )
    println(s"""Buggy line in original file: ${bugInfo("line")}""")

    // Converting to AST can change the code formatting so re-track the line
    val _ast = parseJavaFile(inputFilePath)
    val buggyNode = nodeFromLineNumber(_ast.asInstanceOf[Node], bugInfo("line").toInt)
    val bugLineId = 567876545
    println(s"Detected buggy Node: $buggyNode")
    buggyNode.setComment(new LineComment(s"$bugLineId")) // mark the buggy line with a unique comment
    println(s"Inserted identifier for buggy node: $bugLineId")

    // Write to new file
    val tmp = s"$dir/BuggyExampleIntermediate.txt"
    writeASTToFile(_ast, tmp)

    // Read the re-formatted file and search for buggy line's new position
    val ast = parseJavaFile(tmp)
    val newBuggyNode = ast.findAll(buggyNode.getClass).find {
      case n if n.getComment.isPresent && n.getComment.get().getContent == bugLineId.toString => true
      case _ => false
    }
    val newBuggyLine = newBuggyNode.get.getRange.get().end.line
    println(s"Buggy node in intermediate file: $newBuggyNode")
    println(s"Buggy node ends at line: $newBuggyLine")

    // Insert comments
    val (newAst, newLine) = insertCommentsIntoAST(ast, comments, max_inserts, bugInfo.updated("line", newBuggyLine.toString))
    println(s"Buggy line after mutation: $newLine")
    val mutatedFile1 = s"$dir/BuggyExampleComments.txt"


    writeASTToFile(newAst, mutatedFile1)
    val ast2 = parseJavaFile(mutatedFile1)

    // Insert dead code
    val (newAst2, newLine2) = insertDeadCode(ast2, snippets, max_inserts, bugInfo.updated("line", newLine.toString))
    val mutatedFile2 = s"$dir/BuggyExampleDeadCode.txt"
    println(s"Buggy line after mutation2: $newLine2")

    writeASTToFile(newAst2, mutatedFile2)
    val ast3 = parseJavaFile(mutatedFile2)

    // Insert misleading vars
    val (newAst3, newLine3) = insertMisleadingVars(ast3, varNames, max_inserts, bugInfo.updated("line", newLine2.toString))
    val mutatedFile3 = s"$dir/BuggyExampleMisleadingVars.txt"
    println(s"Buggy line after mutation3: $newLine3")

    writeASTToFile(newAst3, mutatedFile3)

    println(s"AST written back to source and saved at: $outputFilePath")
    println(f"Seed: $seed")

  }



}

/*
{
    "configs": {
      "seed": "seedlab-vt"
    },
    "mutations": {
      "dead_code": {
        "max_inserts": 4,
        "snippets": [
          "if (false) {\n    int tempVal = 999;\n    tempVal -= 1;\n    System.out.println(\"This block is never executed\");\n}",
          "int placeholder = 123;\nplaceholder += 0;\nif (placeholder > 9999) {\n    System.out.println(\"Completely unused check\");\n}",
          "for (int i = 0; i < 3; i++) {}\nboolean dummyCondition = true;\nif (dummyCondition && false) {\n    System.out.println(\"Logic that won't run\");\n}",
          "int result = 42;\nresult += 0;\nif (!(result < 0)) {}\nSystem.out.println(\"# This line won't affect anything\");"
        ]
      },
      "misleading_comments": {
        "max_inserts": 4,
        "comments": [
          "// Ensures parallel universe coherence remains stable",
          "// This line triggers advanced warp field calibration",
          "// Optimizes quantum chromodynamics at runtime",
          "// Secretly reconfigures matrix multiplication for negative time cycles"
        ]
      },
      "misleading_variables": {
        "max_inserts": 5,
        "variables": [
          "serializerDragon",
          "classMorph",
          "quantumMatcher",
          "warpKey",
          "enumGuardian"
        ]
      },
      "decompose": {
        "max_inserts": 1
      }
    }
  }


  {
    "reason": "Misplaced return",
    "line": 37
}
 */
