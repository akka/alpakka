/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.xml.scaladsl

import akka.NotUsed
import akka.stream.alpakka.xml.ParseEvent
import akka.stream.alpakka.xml.impl
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import com.fasterxml.aalto.AsyncXMLInputFactory
import org.w3c.dom.Element

import javax.xml.stream.XMLInputFactory
import javax.xml.XMLConstants

import scala.collection.immutable

object XmlParsing {

  /**
   * The default factory configuration is to enable secure processing.
   */
  val configureDefault: AsyncXMLInputFactory => Unit = { factory =>
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false)
    factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false)
    if (factory.isPropertySupported(XMLConstants.FEATURE_SECURE_PROCESSING)) {
      factory.setProperty(XMLConstants.FEATURE_SECURE_PROCESSING, true)
    }
  }

  /**
   * Parser Flow that takes a stream of ByteStrings and parses them to XML events similar to SAX.
   */
  val parser: Flow[ByteString, ParseEvent, NotUsed] = parser(ignoreInvalidChars = false, configureDefault)

  /**
   * Parser Flow that takes a stream of ByteStrings and parses them to XML events similar to SAX.
   */
  def parser(ignoreInvalidChars: Boolean = false,
             configureFactory: AsyncXMLInputFactory => Unit = configureDefault): Flow[ByteString, ParseEvent, NotUsed] =
    Flow.fromGraph(new impl.StreamingXmlParser(ignoreInvalidChars, configureFactory))

  /**
   * A Flow that transforms a stream of XML ParseEvents. This stage coalesces consecutive CData and Characters
   * events into a single Characters event or fails if the buffered string is larger than the maximum defined.
   */
  def coalesce(maximumTextLength: Int): Flow[ParseEvent, ParseEvent, NotUsed] =
    Flow.fromGraph(new impl.Coalesce(maximumTextLength))

  /**
   * A Flow that transforms a stream of XML ParseEvents. This stage filters out any event not corresponding to
   * a certain path in the XML document. Any event that is under the specified path (including subpaths) is passed
   * through.
   */
  def subslice(path: immutable.Seq[String]): Flow[ParseEvent, ParseEvent, NotUsed] =
    Flow.fromGraph(new impl.Subslice(path))

  /**
   * A Flow that transforms a stream of XML ParseEvents. This stage pushes elements of a certain path in
   * the XML document as org.w3c.dom.Element.
   */
  def subtree(path: immutable.Seq[String]): Flow[ParseEvent, Element, NotUsed] =
    Flow.fromGraph(new impl.Subtree(path))

}
