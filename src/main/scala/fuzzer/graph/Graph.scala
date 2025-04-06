package fuzzer.graph

import fuzzer.code.SourceCode

import scala.collection.mutable

case class Graph[T](
                     nodes: List[Node[T]]
                   ) {

  private val topoSortedNodes = this.kahnTopoSort
  private def roots: List[Node[T]] = nodes.filter(_.parents.isEmpty)

  def bfsBackwardsFromFinal(printNode: Node[T] => Unit): Unit = {
    val visited = scala.collection.mutable.Set[Node[T]]()

    println(s"nodes = ${nodes.mkString(",")}")
    val finalNode = nodes.find(_.children.isEmpty).get

    val queue = scala.collection.mutable.Queue[Node[T]](finalNode)

    while (queue.nonEmpty) {
      val current = queue.dequeue()
      if (!visited.contains(current)) {
        visited += current
        printNode(current)
        current.parents.foreach(queue.enqueue)
      }
    }
  }

  def getSourceNodes: List[Node[T]] = {
    this.nodes.filter(_.parents.isEmpty)
  }

  private def kahnTopoSort: List[Node[T]] = {
    val inDegree = mutable.Map[Node[T], Int]()
    val queue = mutable.Queue[Node[T]]()
    val sorted = mutable.ListBuffer[Node[T]]()

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
        sorted += current
        visited += current

        for (child <- current.children) {
          inDegree(child) -= 1
          if (inDegree(child) == 0) {
            queue.enqueue(child)
          }
        }
      }
    }

    sorted.toList
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

  def traverseTopological(visit: Node[T] => Unit): Unit = this.topoSortedNodes.foreach(visit)
  def transformNodes(visit: Node[T] => T): Graph[T] = {
    Graph(this.topoSortedNodes.map { node =>
      val newNode = Node(visit(node))
      newNode.copyStateFrom(node)
      newNode
    })
  }

}