package transform

import java.nio.file.{Files, Paths}
import scala.meta._

object ScalaASTTransformerSentinel2 {

  def transform(sourceFile: String): String = {
    // Read and parse the source file
    val source = new String(Files.readAllBytes(Paths.get(sourceFile)))
    val tree = source.parse[Source].get

    // Transform the tree
    val transformedTree = transformTree(tree)

    // Return the modified code as text
    transformedTree.syntax
  }

  private def transformTree(tree: Tree): Tree = {
    // Use two passes to ensure we don't miss any transformations
    def transform1(tree: Tree): Tree = {
      tree.transform {
        // First pass: Transform the else parts of all if statements
        case ifTerm @ Term.If(cond, thenp, elsep) =>
          val newElsep = transformElsePart(elsep)
          Term.If(cond, thenp, newElsep)
      }
    }

    // Apply the first pass
    val firstPass = transform1(tree)

    // Apply the second pass
    firstPass.transform {
      // Transform if expressions: if(expr) => if(sentinel(true, "coverageString") && expr)
      case ifTerm @ Term.If(cond, thenp, elsep) =>
        // Find coverage string in the then part
        val thenCoverageString = extractCoverageString(thenp).getOrElse("unknown")

        // Create the new condition with sentinel
        val newCond = q"sentinel(true, ${Lit.String(thenCoverageString)}) && $cond"

        // Return transformed if statement
        Term.If(newCond, thenp, elsep)

      // Transform case expressions
      case c @ Case(pat, guardOpt, body) =>
        // Find coverage string in the case body
        val coverageString = extractCoverageString(body).getOrElse("unknown")

        guardOpt match {
          // Case with no guard: case expr => ... becomes case expr if sentinel(true, "coverage") => ...
          case None =>
            Case(pat, Some(q"sentinel(true, ${Lit.String(coverageString)})"), body)

          // Case with guard: case expr if expr2 => ... becomes case expr if sentinel(true, "coverage") && expr2 => ...
          case Some(guard) =>
            val newGuard = q"sentinel(true, ${Lit.String(coverageString)}) && $guard"
            Case(pat, Some(newGuard), body)
        }
    }
  }

  // Helper method to transform the else part of an if statement
  private def transformElsePart(elsep: Term): Term = {
    elsep match {
      // For an else-if, we'll need to handle this in a different way
      case ifElse: Term.If =>
        val elseCoverageString = extractCoverageString(ifElse.thenp).getOrElse("unknown")
        val newCond = q"sentinel(true, ${Lit.String(elseCoverageString)}) && ${ifElse.cond}"
        Term.If(newCond, ifElse.thenp, transformElsePart(ifElse.elsep))

      // For empty block, add sentinel
      case Term.Block(Nil) =>
        Term.Block(List(q"sentinel(true, ${Lit.String("unknown")})"))

      // For else block with statements
      case Term.Block(stats) =>
        // Extract coverage string if present
        val coverageOpt = if (stats.nonEmpty && isCoverageStatement(stats.head)) {
          extractCoverageFromStatement(stats.head)
        } else {
          None
        }

        val coverageString = coverageOpt.getOrElse("unknown")
        val sentinelCall = q"sentinel(true, ${Lit.String(coverageString)})"

        // Keep the original coverage statement and add sentinel right after it
        val newStats = if (stats.nonEmpty && isCoverageStatement(stats.head)) {
          stats.head +: sentinelCall +: stats.tail
        } else {
          sentinelCall +: stats
        }

        Term.Block(newStats)

      // For a single statement (not a block)
      case singleStat =>
        // If it's a coverage statement, keep it and add sentinel after
        if (isCoverageStatement(singleStat)) {
          val coverageStr = extractCoverageFromStatement(singleStat).getOrElse("unknown")
          Term.Block(List(
            singleStat,
            q"sentinel(true, ${Lit.String(coverageStr)})"
          ))
        } else {
          // Create a block with sentinel followed by statement
          Term.Block(List(
            q"sentinel(true, ${Lit.String("unknown")})",
            singleStat
          ))
        }
    }
  }

  // Helper to check if a statement is a coverage statement
  private def isCoverageStatement(stat: Tree): Boolean = {
    stat match {
      case Term.ApplyInfix(
      Term.Select(_, Term.Name("coverage")),
      Term.Name("+="),
      _,
      List(Lit.String(_))
      ) => true
      case _ => false
    }
  }

  // Helper to extract coverage string from a statement
  private def extractCoverageFromStatement(stat: Tree): Option[String] = {
    stat match {
      case Term.ApplyInfix(
      Term.Select(_, Term.Name("coverage")),
      Term.Name("+="),
      _,
      List(Lit.String(coverageStr))
      ) => Some(coverageStr)
      case _ => None
    }
  }

  // Helper to extract coverage string from a block or statement
  private def extractCoverageString(tree: Tree): Option[String] = {
    tree match {
      case Term.Block(firstStat :: _) if isCoverageStatement(firstStat) =>
        extractCoverageFromStatement(firstStat)
      case stat if isCoverageStatement(stat) =>
        extractCoverageFromStatement(stat)
      case _ => None
    }
  }

  def main(args: Array[String]): Unit = {

    val inputFile = if (args.length > 1) args(0) else "/home/ahmad/Documents/project/spark/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala"
    val outputFile = if (args.length > 2) args(1) else "/home/ahmad/Documents/project/cardinality-estimator-test/src/main/scala/transform/tmp/sentinals.scala"

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