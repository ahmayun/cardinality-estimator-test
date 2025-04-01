//package transform
//
//import java.nio.file.{Files, Paths}
//import scala.meta._
//
//object ScalaASTTransformer5 {
//  case class Context(className: String, methodName: String)
//
//  def transform(sourceFile: String): String = {
//    // Read and parse the source file
//    val source = new String(Files.readAllBytes(Paths.get(sourceFile)))
//    val tree = source.parse[Source].get
//
//    def findMethodName(t: Tree): Option[String] = {
//      t match {
//        case d: Defn.Def => Some(d.name.value)
//        case _ => t.parent.flatMap(findMethodName)
//      }
//    }
//
//    def findClassName(t: Tree): Option[String] = {
//      t match {
//        case o: Defn.Object => Some(o.name.value)
//        case c: Defn.Class => Some(c.name.value)
//        case t: Defn.Trait => Some(t.name.value)
//        case _ => t.parent.flatMap(findClassName)
//      }
//    }
//
//    def findInitialContext(tree: Tree): Context = {
//      val methodName = findMethodName(tree).getOrElse("Unknown")
//      val className = findClassName(tree).getOrElse("Unknown")
//      Context(className, methodName)
//    }
//
//    // Counter for unique branch identification within each context
//    var branchCounter = Map[Context, Int]()
//
//    def getNextBranchId(context: Context): Int = {
//      val count = branchCounter.getOrElse(context, 0) + 1
//      branchCounter = branchCounter.updated(context, count)
//      count
//    }
//
//    def createPrintlnStmt(context: Context, branchType: String, branchId: Int): Term = {
//      val message = s"${context.className}.${context.methodName}.$branchType$branchId"
//      q"println(${Lit.String(message)})"
//    }
//
//    def transformWithContext(tree: Tree, context: Option[Context]): Tree = {
//      val currentContext = context.getOrElse(findInitialContext(tree))
//
//      tree match {
//        // Handle partial functions (like those used in transform)
//        case pf @ Term.PartialFunction(cases) =>
//          val newCases = cases.map { case c @ Case(pat, guard, body) =>
//            val branchId = getNextBranchId(currentContext)
//            val printlnStmt = createPrintlnStmt(currentContext, "case", branchId)
//
//            val newBody = body match {
//              case block: Term.Block => Term.Block(printlnStmt +: block.stats)
//              case expr => Term.Block(List(printlnStmt, expr))
//            }
//            Case(pat, guard, newBody)
//          }
//          Term.PartialFunction(newCases)
//
//        // Handle if expressions
//        case ifExpr @ Term.If(cond, thenp, elsep) =>
//          def addPrintlnToBlock(expr: Term, branchType: String): Term.Block = {
//            val branchId = getNextBranchId(currentContext)
//            val printlnStmt = createPrintlnStmt(currentContext, branchType, branchId)
//
//            expr match {
//              case block: Term.Block => Term.Block(printlnStmt +: block.stats)
//              case expr => Term.Block(List(printlnStmt, expr))
//            }
//          }
//
//          def processElseBranch(elsep: Term): Term = elsep match {
//            case Term.If(nestedCond, nestedThenp, nestedElsep) =>
//              // Handle else-if without recursively transforming
//              Term.If(nestedCond,
//                addPrintlnToBlock(nestedThenp, "elseif"),
//                processElseBranch(nestedElsep))
//            case Lit.Unit() =>
//              // No else branch, add one with println
//              val branchId = getNextBranchId(currentContext)
//              val printlnStmt = createPrintlnStmt(currentContext, "else", branchId)
//              Term.Block(List(printlnStmt))
//            case expr =>
//              // Regular else branch, add println
//              addPrintlnToBlock(expr, "else")
//          }
//
//          Term.If(cond, addPrintlnToBlock(thenp, "then"), processElseBranch(elsep))
//
//        // Handle match expressions
//        case matchExpr @ Term.Match(expr, cases) =>
//          val newCases = cases.map { case c @ Case(pat, guard, body) =>
//            val branchId = getNextBranchId(currentContext)
//            val printlnStmt = createPrintlnStmt(currentContext, "match", branchId)
//
//            val newBody = body match {
//              case block: Term.Block => Term.Block(printlnStmt +: block.stats)
//              case expr => Term.Block(List(printlnStmt, expr))
//            }
//            Case(pat, guard, newBody)
//          }
//          Term.Match(expr, newCases)
//
//        // For other nodes that we care about, transform only specific children
//        case Term.Function(params, body) =>
//          Term.Function(params, transformWithContext(body, Some(currentContext)).asInstanceOf[Term])
//
//        case Term.Block(stats) =>
//          Term.Block(stats.map(stat => transformWithContext(stat, Some(currentContext)).asInstanceOf[Term]))
//
//        case defn @ Defn.Def(_, name, _, _, _, body) =>
//          // Update context when entering a new method
//          val newContext = Context(currentContext.className, name.value)
//          defn.copy(body = transformWithContext(body, Some(newContext)).asInstanceOf[Term])
//
//        case obj @ Defn.Object(_, name, _) =>
//          // Update context when entering a new object/class
//          val newContext = Context(name.value, currentContext.methodName)
//          obj
//
//        case cls @ Defn.Class(_, name, _, _, _) =>
//          // Update context when entering a new object/class
//          val newContext = Context(name.value, currentContext.methodName)
//          cls
//
//        case trt @ Defn.Trait(_, name, _, _, _) =>
//          // Update context when entering a new object/class
//          val newContext = Context(name.value, currentContext.methodName)
//          trt
//
//        // For any other node, just return it unchanged
//        case other => other
//      }
//    }
//
//    // Start the transformation with no initial context
//    val transformed = transformWithContext(tree, None)
//
//    // Pretty print the transformed tree
//    transformed.syntax
//  }
//
//  def main(args: Array[String]): Unit = {
//
//    val inputFile = if (args.length > 1) args(0) else "/home/ahmad/Documents/project/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala"
//    val outputFile = if (args.length > 2) args(1) else "/home/ahmad/Documents/project/cardinality-estimator-test/src/main/scala/transform/tmp/test5.scala"
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
