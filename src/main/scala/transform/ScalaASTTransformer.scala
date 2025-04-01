//package transform
//
//import scala.meta._
//import java.nio.file.{Files, Paths}
//
//object ScalaASTTransformer {
//  def transform(sourceFile: String): String = {
//    // Read and parse the source file
//    val source = new String(Files.readAllBytes(Paths.get(sourceFile)))
//    val tree = source.parse[Source].get
//
//    // Transform the tree
//    val transformed = tree.transform {
//      // Handle if expressions
//      case ifExpr @ Term.If(cond, thenp, elsep) =>
//        val printlnStmt = q"println(${Lit.String("Hello")})"
//
//        val newThenp = thenp match {
//          case block: Term.Block => Term.Block(printlnStmt +: block.stats)
//          case expr => Term.Block(List(printlnStmt, expr))
//        }
//
//        val newElsep = elsep match {
//          case Lit.Unit() => // No else branch
//            Term.Block(List(printlnStmt))
//          case block: Term.Block =>
//            Term.Block(printlnStmt +: block.stats)
//          case expr =>
//            Term.Block(List(printlnStmt, expr))
//        }
//
//        Term.If(cond, newThenp, newElsep)
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
//    val outputFile = if (args.length > 2) args(1) else "/home/ahmad/Documents/project/cardinality-estimator-test/src/main/scala/transform/tmp/test.scala"
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