/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.xml

import java.io.{ BufferedInputStream, File, FileInputStream, InputStream }

import akka.NotUsed
import akka.stream.scaladsl.Source

import scala.io.{ Source => ScalaIOSource }
import scala.xml.pull.{ XMLEvent, XMLEventReader }

object XmlEventSource {
  def fromInputStream(xml: InputStream): Source[XMLEvent, NotUsed] =
    Source.fromIterator(() => new XMLEventReader(ScalaIOSource.fromInputStream(xml)))

  def fromFile(file: File): Source[XMLEvent, NotUsed] =
    fromInputStream(new BufferedInputStream(new FileInputStream(file)))

  def fromFileName(name: String): Source[XMLEvent, NotUsed] =
    fromInputStream(new BufferedInputStream(new FileInputStream(name)))
}
