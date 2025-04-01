package transform

import java.nio.file.{Files, Paths}
import scala.meta._

object ScalaASTTransformer4 {
  case class Context(className: String, methodName: String)

  def transform(sourceFile: String): String = {
    // Read and parse the source file
    val source = new String(Files.readAllBytes(Paths.get(sourceFile)))
    val tree = source.parse[Source].get

    def findContext(tree: Tree): Context = {
      def findMethodName(t: Tree): Option[String] = {
        t match {
          case d: Defn.Def => Some(d.name.value)
          case _ => t.parent.flatMap(findMethodName)
        }
      }

      def findClassName(t: Tree): Option[String] = {
        t match {
          case o: Defn.Object => Some(o.name.value)
          case c: Defn.Class => Some(c.name.value)
          case t: Defn.Trait => Some(t.name.value)
          case _ => t.parent.flatMap(findClassName)
        }
      }

      // Start from the current tree and look up for both class and method names
      val methodName = findMethodName(tree).getOrElse("Unknown")
      val className = findClassName(tree).getOrElse("Unknown")

      Context(className, methodName)
    }

    // Counter for unique branch identification within each context
    var branchCounter = Map[Context, Int]()

    def getNextBranchId(context: Context): Int = {
      val count = branchCounter.getOrElse(context, 0) + 1
      branchCounter = branchCounter.updated(context, count)
      count
    }

    def createPrintlnStmt(context: Context, branchType: String, branchId: Int): Term = {
      val message = s"${context.className}.${context.methodName}.$branchType$branchId"
      q"println(${Lit.String(message)})"
    }

    // Transform the tree
    val transformed = tree.transform {
      // Handle partial functions (like those used in transform)
      case pf @ Term.PartialFunction(cases) =>
        val context = findContext(pf)

        val newCases = cases.zipWithIndex.map { case (c @ Case(pat, guard, body), idx) =>
          val branchId = getNextBranchId(context)
          val printlnStmt = createPrintlnStmt(context, "case", branchId)

          val newBody = body match {
            case block: Term.Block => Term.Block(printlnStmt +: block.stats)
            case expr => Term.Block(List(printlnStmt, expr))
          }
          Case(pat, guard, newBody)
        }

        Term.PartialFunction(newCases)

      // Handle if expressions
      case ifExpr @ Term.If(cond, thenp, elsep) =>
        val context = findContext(ifExpr)

        def addPrintlnToBlock(expr: Term, branchType: String): Term.Block = {
          val branchId = getNextBranchId(context)
          val printlnStmt = createPrintlnStmt(context, branchType, branchId)

          expr match {
            case block: Term.Block => Term.Block(printlnStmt +: block.stats)
            case expr => Term.Block(List(printlnStmt, expr))
          }
        }

        def processElseBranch(elsep: Term): Term = elsep match {
          case Term.If(nestedCond, nestedThenp, nestedElsep) =>
            // Handle else-if without recursively transforming
            Term.If(nestedCond,
              addPrintlnToBlock(nestedThenp, "elseif"),
              processElseBranch(nestedElsep))
          case Lit.Unit() =>
            // No else branch, add one with println
            val branchId = getNextBranchId(context)
            val printlnStmt = createPrintlnStmt(context, "else", branchId)
            Term.Block(List(printlnStmt))
          case expr =>
            // Regular else branch, add println
            addPrintlnToBlock(expr, "else")
        }

        // Transform the then branch and handle the else chain
        Term.If(cond, addPrintlnToBlock(thenp, "then"), processElseBranch(elsep))

      // Handle match expressions
      case matchExpr @ Term.Match(expr, cases) =>
        val context = findContext(matchExpr)

        val newCases = cases.zipWithIndex.map { case (c @ Case(pat, guard, body), idx) =>
          val branchId = getNextBranchId(context)
          val printlnStmt = createPrintlnStmt(context, "match", branchId)

          val newBody = body match {
            case block: Term.Block => Term.Block(printlnStmt +: block.stats)
            case expr => Term.Block(List(printlnStmt, expr))
          }
          Case(pat, guard, newBody)
        }

        Term.Match(expr, newCases)
    }

    // Pretty print the transformed tree
    transformed.syntax
  }

  def main(args: Array[String]): Unit = {

    val inputFile = if (args.length > 1) args(0) else "/home/ahmad/Documents/project/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala"
    val outputFile = if (args.length > 2) args(1) else "/home/ahmad/Documents/project/cardinality-estimator-test/src/main/scala/transform/tmp/test4.scala"


    try {
      val transformedCode = transform(inputFile)
      Files.write(Paths.get(outputFile), transformedCode.getBytes)
      println(s"Successfully transformed $inputFile to $outputFile")
    } catch {
      case e: Exception =>
        println(s"Error processing file: ${e.getMessage}")
        System.exit(1)
    }
  }
}
