package fuzzer.graph

import scala.collection.mutable

case class Node[T](value: T) {
  var children: List[Node[T]] = List()
  var parents: List[Node[T]] = List()
  val reachableFromSources: mutable.Set[Node[T]] = mutable.Set()

  def traverseDescendants: List[T] = value :: children.flatMap(_.traverseDescendants)
  def traverseAncestors: List[T] = value :: parents.flatMap(_.traverseAncestors)
  def hasAncestors: Boolean = parents.nonEmpty
  def isUnary: Boolean = parents.length == 1
  def isBinary: Boolean = parents.length == 2
  def getInDegree: Int = parents.length
  def getOutDegree: Int = children.length
  def copyStateFrom(other: Node[T]): Unit = {
    children = other.children.map(identity)
    parents = other.parents.map(identity)
  }

  override def toString: String = value.toString
}