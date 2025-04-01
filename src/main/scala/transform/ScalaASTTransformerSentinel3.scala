package transform
// If hte
import java.nio.file.{Files, Paths}
import scala.meta._

object ScalaASTTransformerSentinel3 {

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

    // Helper method to identify the last case in a match pattern
    def isLastCase(cases: List[Case], currentCase: Case): Boolean = {
      cases.lastOption.contains(currentCase)
    }

    // Apply the first pass
    val firstPass = transform1(tree)

    // Apply the second pass
    firstPass.transform {
      // Handle match statements to identify last case
      case t @ Term.Match(expr, cases) =>
        val newCases = cases.map { c =>
          if (isLastCase(cases, c)) {
            // For the last case, add sentinel to the body instead of guard
            val coverageString = extractCoverageString(c.body).getOrElse("unknown")
            val sentinelLit = Lit.String(coverageString)
            val sentinelCall = Term.Apply(Term.Name("sentinel"), List(Lit.Boolean(true), sentinelLit))

            c.body match {
              case Term.Block(stats) =>
                // If the body has a coverage statement at the beginning, add after it
                val newStats = if (stats.nonEmpty && isCoverageStatement(stats.head)) {
                  stats.head +: sentinelCall +: stats.tail
                } else {
                  sentinelCall +: stats
                }
                Case(c.pat, c.cond, Term.Block(newStats))

              // For single statement body
              case other =>
                if (isCoverageStatement(other)) {
                  Case(c.pat, c.cond, Term.Block(List(other, sentinelCall)))
                } else {
                  Case(c.pat, c.cond, Term.Block(List(sentinelCall, other)))
                }
            }
          } else {
            // For non-last cases, continue with the previous approach
            val coverageString = extractCoverageString(c.body).getOrElse("unknown")
            c.cond match {
              case None =>
                val sentinelLit = Lit.String(coverageString)
                val sentinelCall = Term.Apply(Term.Name("sentinel"), List(Lit.Boolean(true), sentinelLit))
                Case(c.pat, Some(sentinelCall), c.body)
              case Some(guard) =>
                val sentinelLit = Lit.String(coverageString)
                val sentinelCall = Term.Apply(Term.Name("sentinel"), List(Lit.Boolean(true), sentinelLit))
                val newGuard = Term.ApplyInfix(sentinelCall, Term.Name("&&"), Nil, List(guard))
                Case(c.pat, Some(newGuard), c.body)
            }
          }
        }
        Term.Match(expr, newCases)

      // Transform if expressions: if(expr) => if(sentinel(true, "coverageString") && expr)
      case ifTerm @ Term.If(cond, thenp, elsep) =>
        // Find coverage string in the then part
        val thenCoverageString = extractCoverageString(thenp).getOrElse("unknown")

        // Create the new condition with sentinel
        val sentinelLit = Lit.String(thenCoverageString)
        val sentinelCall = Term.Apply(Term.Name("sentinel"), List(Lit.Boolean(true), sentinelLit))
        val newCond = Term.ApplyInfix(sentinelCall, Term.Name("&&"), Nil, List(cond))

        // Return transformed if statement
        Term.If(newCond, thenp, elsep)

      // Transform case expressions in other contexts (not in match statements)
      case c @ Case(pat, cond, body) if !c.parent.exists(_.is[Term.Match]) =>
        // Find coverage string in the case body
        val coverageString = extractCoverageString(body).getOrElse("unknown")

        cond match {
          // Case with no guard
          case None =>
            Case(pat, Some(q"sentinel(true, ${Lit.String(coverageString)})"), body)

          // Case with guard
          case Some(guard) =>
            val sentinelLit = Lit.String(coverageString)
            val sentinelCall = Term.Apply(Term.Name("sentinel"), List(Lit.Boolean(true), sentinelLit))
            val newGuard = Term.ApplyInfix(sentinelCall, Term.Name("&&"), Nil, List(guard))
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
        val sentinelLit = Lit.String(elseCoverageString)
        val sentinelCall = Term.Apply(Term.Name("sentinel"), List(Lit.Boolean(true), sentinelLit))
        val newCond = Term.ApplyInfix(sentinelCall, Term.Name("&&"), Nil, List(ifElse.cond))
        Term.If(newCond, ifElse.thenp, transformElsePart(ifElse.elsep))

      // For empty block, add sentinel
      case Term.Block(Nil) =>
        val sentinelLit = Lit.String("unknown")
        val sentinelCall = Term.Apply(Term.Name("sentinel"), List(Lit.Boolean(true), sentinelLit))
        Term.Block(List(sentinelCall))

      // For else block with statements
      case Term.Block(stats) =>
        // Extract coverage string if present
        val coverageOpt = if (stats.nonEmpty && isCoverageStatement(stats.head)) {
          extractCoverageFromStatement(stats.head)
        } else {
          None
        }

        val coverageString = coverageOpt.getOrElse("unknown")
        val sentinelLit = Lit.String(coverageString)
        val sentinelCall = Term.Apply(Term.Name("sentinel"), List(Lit.Boolean(true), sentinelLit))

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
          val sentinelLit = Lit.String(coverageStr)
          val sentinelCall = Term.Apply(Term.Name("sentinel"), List(Lit.Boolean(true), sentinelLit))
          Term.Block(List(
            singleStat,
            sentinelCall
          ))
        } else {
          // Create a block with sentinel followed by statement
          val sentinelLit = Lit.String("unknown")
          val sentinelCall = Term.Apply(Term.Name("sentinel"), List(Lit.Boolean(true), sentinelLit))
          Term.Block(List(
            sentinelCall,
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