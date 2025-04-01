package fuzzer.graph

import fuzzer.code.SourceCode
import scala.collection.mutable

case class Graph[T](nodes: List[Node[T]]) {
  val topoSortedNodes = this.topologicalSort
  def roots: List[Node[T]] = nodes.filter(_.parents.isEmpty)
  def traverseAll: List[T] = nodes.flatMap(_.traverseDescendants).distinct
  def bfsBackwardsFromFinal(printNode: Node[T] => Unit): Unit = {
    val visited = scala.collection.mutable.Set[Node[T]]()

    val finalNode = nodes.find(_.children.isEmpty).get

    val queue = scala.collection.mutable.Queue[Node[T]](finalNode)

    while (queue.nonEmpty) {
      val current = queue.dequeue()
      if (!visited.contains(current)) {
        visited += current
        printNode(current)
        current.parents.foreach(queue.enqueue(_))
      }
    }
  }

  def kahnTraverse(visit: Node[T] => Unit): Unit = {
    val inDegree = mutable.Map[Node[T], Int]()
    val queue = mutable.Queue[Node[T]]()

    // Initialize in-degrees
    for (node <- nodes) {
      inDegree(node) = node.parents.size
      if (node.parents.isEmpty) {
        queue.enqueue(node)
      }
    }

    val visited = mutable.Set[Node[T]]()

    while (queue.nonEmpty) {
      val current = queue.dequeue()
      if (!visited.contains(current)) {
        visit(current)
        visited += current

        for (child <- current.children) {
          inDegree(child) -= 1
          if (inDegree(child) == 0) {
            queue.enqueue(child)
          }
        }
      }
    }
  }

  private def topologicalSort: List[Node[T]] = {
    val visited = mutable.Set[Node[T]]()
    val result = mutable.ListBuffer[Node[T]]()

    def dfs(node: Node[T]): Unit = {
      if (!visited.contains(node)) {
        visited += node
        node.children.foreach(dfs)
        result.prepend(node)
      }
    }

    this.nodes.foreach(dfs)

    result.toList
  }

  def generateCode(generator: Graph[T] => SourceCode): SourceCode = {
    generator(this)
  }

  def computeReachabilityFromSources(): Unit = {
    val graph = this

    val sources = graph.roots
    sources.foreach(source => source.reachableFromSources += source)

    for (node <- this.topoSortedNodes) {
      for (child <- node.children) {
        child.reachableFromSources ++= node.reachableFromSources
      }
    }
  }
}