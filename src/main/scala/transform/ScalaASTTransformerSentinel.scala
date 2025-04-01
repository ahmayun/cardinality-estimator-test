package transform

import java.nio.file.{Files, Paths}
import scala.meta._

object ScalaASTTransformerSentinel {

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
    tree.transform {
      // Transform if expressions
      case ifTerm @ Term.If(cond, thenp, elsep) =>
        // Look for coverage statement at the beginning of the then block
        val coverageString = extractCoverageString(thenp)
        val sentinelArg = coverageString.getOrElse("unknown")

        val newCond = q"sentinel(true, ${Lit.String(sentinelArg)}) && $cond"
        Term.If(newCond, thenp, elsep)

      // Transform case expressions
      case c @ Case(pat, guardOpt, body) =>
        // Look for coverage statement at the beginning of the case body
        val coverageString = extractCoverageString(body)
        val sentinelArg = coverageString.getOrElse("unknown")

        guardOpt match {
          // Case with no guard
          case None =>
            Case(pat, Some(q"sentinel(true, ${Lit.String(sentinelArg)})"), body)

          // Case with guard
          case Some(guard) =>
            val newGuard = q"sentinel(true, ${Lit.String(sentinelArg)}) && $guard"
            Case(pat, Some(newGuard), body)
        }
    }
  }

  // Helper method to extract the coverage string from the beginning of a block
  private def extractCoverageString(tree: Tree): Option[String] = {
    tree match {
      // If the block starts with a coverage statement
      case Term.Block(firstStat :: _) =>
        extractCoverageFromStatement(firstStat)

      // If it's a single statement (not a block)
      case stat =>
        extractCoverageFromStatement(stat)
    }
  }

  private def extractCoverageFromStatement(stat: Tree): Option[String] = {
    stat match {
      // Match statements like: Rule.coverage += "RemoveNoopOperators.apply.else23"
      case Term.ApplyInfix(
      Term.Select(_, Term.Name("coverage")),
      Term.Name("+="),
      _,
      List(Lit.String(coverageStr))
      ) => Some(coverageStr)

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