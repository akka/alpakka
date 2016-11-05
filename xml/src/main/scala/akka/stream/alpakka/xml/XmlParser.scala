/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.xml

import akka.stream.scaladsl.Flow

import scala.collection.immutable.Iterable
import scala.util.Try
import scala.xml.MetaData
import scala.xml.pull.XMLEvent

object XmlParser {
  def emit[A](elems: A*): Iterable[A] = Iterable[A](elems: _*)
  def getAttr(meta: MetaData, default: String = "")(key: String): String =
    meta.asAttrMap.getOrElse(key, default)
  def flow[A](parser: PartialFunction[XMLEvent, Iterable[A]]) = apply(parser)
  def apply[A](parser: PartialFunction[XMLEvent, Iterable[A]]) = {
    Flow[XMLEvent].statefulMapConcat[A](() => xml => Try(parser(xml)).getOrElse(emit()))
  }
}
