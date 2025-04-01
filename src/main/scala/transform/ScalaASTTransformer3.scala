//package transform
//
//import java.nio.file.{Files, Paths}
//import scala.meta._
//
//object ScalaASTTransformer3 {
//  def transform(sourceFile: String): String = {
//    // Read and parse the source file
//    val source = new String(Files.readAllBytes(Paths.get(sourceFile)))
//    val tree = source.parse[Source].get
//
//    // Transform the tree
//    val transformed = tree.transform {
//      // Handle partial functions (like those used in transform)
//      case pf @ Term.PartialFunction(cases) =>
//        val printlnStmt = q"println(${Lit.String("Hello")})"
//
//        val newCases = cases.map { case c @ Case(pat, guard, body) =>
//          val newBody = body match {
//            case block: Term.Block => Term.Block(printlnStmt +: block.stats)
//            case expr => Term.Block(List(printlnStmt, expr))
//          }
//          Case(pat, guard, newBody)
//        }
//
//        Term.PartialFunction(newCases)
//
//      // Handle if expressions
//      case ifExpr @ Term.If(cond, thenp, elsep) =>
//        val printlnStmt = q"println(${Lit.String("Hello")})"
//
//        def addPrintlnToBlock(expr: Term): Term.Block = expr match {
//          case block: Term.Block => Term.Block(printlnStmt +: block.stats)
//          case expr => Term.Block(List(printlnStmt, expr))
//        }
//
//        def processElseBranch(elsep: Term): Term = elsep match {
//          case Term.If(nestedCond, nestedThenp, nestedElsep) =>
//            // Handle else-if without recursively transforming
//            Term.If(nestedCond,
//              addPrintlnToBlock(nestedThenp),
//              processElseBranch(nestedElsep))
//          case Lit.Unit() =>
//            // No else branch, add one with println
//            Term.Block(List(printlnStmt))
//          case expr =>
//            // Regular else branch, add println
//            addPrintlnToBlock(expr)
//        }
//
//        // Transform the then branch and handle the else chain
//        Term.If(cond, addPrintlnToBlock(thenp), processElseBranch(elsep))
//
//      // Handle match expressions
//      case matchExpr @ Term.Match(expr, cases) =>
//        val printlnStmt = q"println(${Lit.String("Hello")})"
//
//        val newCases = cases.map { case c @ Case(pat, guard, body) =>
//          val newBody = body match {
//            case block: Term.Block => Term.Block(printlnStmt +: block.stats)
//            case expr => Term.Block(List(printlnStmt, expr))
//          }
//          Case(pat, guard, newBody)
//        }
//
//        Term.Match(expr, newCases)
//    }
//
//    // Pretty print the transformed tree
//    transformed.syntax
//  }
//
//  def main(args: Array[String]): Unit = {
//
//    val inputFile = if (args.length > 1) args(0) else "/home/ahmad/Documents/project/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala"
//    val outputFile = if (args.length > 2) args(1) else "/home/ahmad/Documents/project/cardinality-estimator-test/src/main/scala/transform/tmp/test3.scala"
//
//
//    try {
//      val transformedCode = transform(inputFile)
//      Files.write(Paths.get(outputFile), transformedCode.getBytes)
//      println(s"Successfully transformed $inputFile to $outputFile")
//    } catch {
//      case e: Exception =>
//        println(s"Error processing file: ${e.getMessage}")
//        System.exit(1)
//    }
//  }
//}
