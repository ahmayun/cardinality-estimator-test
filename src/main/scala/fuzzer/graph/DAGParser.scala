package fuzzer.graph

import java.io.{File, FileInputStream}
import org.yaml.snakeyaml.Yaml
import utils.yaml.YamlReader

import scala.jdk.CollectionConverters._

object DAGParser {
  def parseYamlFile[T](filePath: String, nodeBuilder: Map[String, Any] => T): Graph[T] = {
    val data = YamlReader.readYamlFile(filePath)

    val nodesRaw = data("nodes").asInstanceOf[java.util.List[java.util.Map[String, Any]]].asScala
    val linksRaw = data("links").asInstanceOf[java.util.List[java.util.Map[String, Any]]].asScala

    // Step 1: Create all Node[T] instances with no edges
    val nodeMap: Map[Any, Node[T]] = nodesRaw.map(_.asScala.toMap).map { m =>
      val id = m("id")
      val node = Node(nodeBuilder(m))
      id -> node
    }.toMap

    // Step 2: Add child and parent references
    linksRaw.map(_.asScala.toMap).foreach { link =>
      val src = link("source")
      val tgt = link("target")
      val parent = nodeMap(src)
      val child = nodeMap(tgt)
      parent.children ::= child
      child.parents ::= parent
    }

    Graph(nodeMap.values.toList)
  }
}
