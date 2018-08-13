/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.xml

import java.util.Optional

import scala.collection.JavaConverters._
import scala.collection.immutable.DefaultMap
import scala.compat.java8.OptionConverters._

/**
 * XML parsing events emitted by the parser flow. These roughly correspond to Java XMLEvent types.
 */
sealed trait ParseEvent
sealed trait TextEvent extends ParseEvent {
  def text: String
}

case object StartDocument extends ParseEvent {

  /**
   * Java API
   */
  def getInstance(): StartDocument.type = this
}

case object EndDocument extends ParseEvent {

  /**
   * Java API
   */
  def getInstance(): EndDocument.type = this
}

private class MapOverTraversable[A, K, V](source: Traversable[A], fKey: A => K, fValue: A => V)
    extends DefaultMap[K, V] {
  override def get(key: K): Option[V] = source.find(a => fKey(a) == key).map(fValue)

  override def iterator: Iterator[(K, V)] = source.toIterator.map(a => (fKey(a), fValue(a)))

}

final case class Namespace(uri: String, prefix: Option[String] = None)

object Namespace {

  /**
   * Java API
   */
  def create(uri: String, prefix: Optional[String]) =
    Namespace(uri, prefix.asScala)

}

final case class Attribute(name: String, value: String, prefix: Option[String] = None, namespace: Option[String] = None)
object Attribute {

  /**
   * Java API
   */
  def create(name: String, value: String, prefix: Optional[String], namespace: Optional[String]) =
    Attribute(name, value, prefix.asScala, namespace.asScala)

  /**
   * Java API
   */
  def create(name: String, value: String) = Attribute(name, value)
}

final case class StartElement(localName: String,
                              attributesList: List[Attribute] = List.empty[Attribute],
                              prefix: Option[String] = None,
                              namespace: Option[String] = None,
                              namespaceCtx: List[Namespace] = List.empty[Namespace])
    extends ParseEvent {
  val attributes: Map[String, String] =
    new MapOverTraversable[Attribute, String, String](attributesList, _.name, _.value)

  def findAttribute(name: String): Option[Attribute] = attributesList.find(_.name == name)
}

object StartElement {

  def fromMapToAttributeList(prefix: Option[String] = None,
                             namespace: Option[String] = None)(attributes: Map[String, String]): List[Attribute] =
    attributes.toList.map {
      case (name, value) => Attribute(name, value, prefix, namespace)
    }

  def apply(localName: String, attributes: Map[String, String]): StartElement = {
    val attributesList = fromMapToAttributeList()(attributes)
    new StartElement(localName, attributesList, prefix = None, namespace = None, namespaceCtx = List.empty[Namespace])
  }

  /**
   * Java API
   */
  def create(localName: String,
             attributesList: java.util.List[Attribute],
             prefix: Optional[String],
             namespace: Optional[String],
             namespaceCtx: java.util.List[Namespace]): StartElement =
    new StartElement(localName,
                     attributesList.asScala.toList,
                     prefix.asScala,
                     namespace.asScala,
                     namespaceCtx.asScala.toList)

  /**
   * Java API
   */
  def create(localName: String,
             attributesList: java.util.List[Attribute],
             prefix: Optional[String],
             namespace: Optional[String]): StartElement =
    new StartElement(localName, attributesList.asScala.toList, prefix.asScala, namespace.asScala, List.empty[Namespace])

  /**
   * Java API
   */
  def create(localName: String, attributesList: java.util.List[Attribute], namespace: String): StartElement =
    new StartElement(localName,
                     attributesList.asScala.toList,
                     prefix = None,
                     namespace = Some(namespace),
                     namespaceCtx = List(Namespace(namespace)))

  /**
   * Java API
   */
  def create(localName: String, attributes: java.util.Map[String, String]): StartElement =
    StartElement(localName, attributes.asScala.toMap)
}
final case class EndElement(localName: String) extends ParseEvent
object EndElement {

  /**
   * Java API
   */
  def create(localName: String) =
    EndElement(localName)
}
final case class Characters(text: String) extends TextEvent
object Characters {

  /**
   * Java API
   */
  def create(text: String) =
    Characters(text)
}
final case class ProcessingInstruction(target: Option[String], data: Option[String]) extends ParseEvent
object ProcessingInstruction {

  /**
   * Java API
   */
  def create(target: Optional[String], data: Optional[String]) =
    ProcessingInstruction(target.asScala, data.asScala)
}
final case class Comment(text: String) extends ParseEvent
object Comment {

  /**
   * Java API
   */
  def create(text: String) =
    Comment(text)
}
final case class CData(text: String) extends TextEvent
object CData {

  /**
   * Java API
   */
  def create(text: String) =
    CData(text)
}
