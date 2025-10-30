/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.xml.javadsl

import akka.NotUsed
import akka.stream.alpakka.xml
import akka.stream.alpakka.xml.ParseEvent
import akka.util.ByteString
import com.fasterxml.aalto.AsyncXMLInputFactory
import org.w3c.dom.Element

import java.util.function.Consumer

import scala.jdk.CollectionConverters._

object XmlParsing {

  /**
   * Parser Flow that takes a stream of ByteStrings and parses them to XML events similar to SAX.
   */
  def parser(): akka.stream.javadsl.Flow[ByteString, ParseEvent, NotUsed] =
    xml.scaladsl.XmlParsing.parser.asJava

  /**
   * Parser Flow that takes a stream of ByteStrings and parses them to XML events similar to SAX while keeping
   * a context attached.
   */
  def parserWithContext[Ctx](): akka.stream.javadsl.FlowWithContext[ByteString, Ctx, ParseEvent, Ctx, NotUsed] =
    xml.scaladsl.XmlParsing.parserWithContext().asJava

  /**
   * Parser Flow that takes a stream of ByteStrings and parses them to XML events similar to SAX.
   */
  def parser(ignoreInvalidChars: Boolean): akka.stream.javadsl.Flow[ByteString, ParseEvent, NotUsed] =
    xml.scaladsl.XmlParsing.parser(ignoreInvalidChars).asJava

  /**
   * Parser Flow that takes a stream of ByteStrings and parses them to XML events similar to SAX while keeping
   * a context attached.
   */
  def parserWithContext[Ctx](
      ignoreInvalidChars: Boolean
  ): akka.stream.javadsl.FlowWithContext[ByteString, Ctx, ParseEvent, Ctx, NotUsed] =
    xml.scaladsl.XmlParsing.parserWithContext(ignoreInvalidChars).asJava

  /**
   * Parser Flow that takes a stream of ByteStrings and parses them to XML events similar to SAX.
   */
  def parser(
      configureFactory: Consumer[AsyncXMLInputFactory]
  ): akka.stream.javadsl.Flow[ByteString, ParseEvent, NotUsed] =
    xml.scaladsl.XmlParsing.parser(false, configureFactory.accept(_)).asJava

  /**
   * Parser Flow that takes a stream of ByteStrings and parses them to XML events similar to SAX.
   */
  def parser(
      ignoreInvalidChars: Boolean,
      configureFactory: Consumer[AsyncXMLInputFactory]
  ): akka.stream.javadsl.Flow[ByteString, ParseEvent, NotUsed] =
    xml.scaladsl.XmlParsing.parser(ignoreInvalidChars, configureFactory.accept(_)).asJava

  /**
   * Parser Flow that takes a stream of ByteStrings and parses them to XML events similar to SAX while keeping
   * a context attached.
   */
  def parserWithContext[Ctx](
      ignoreInvalidChars: Boolean,
      configureFactory: Consumer[AsyncXMLInputFactory]
  ): akka.stream.javadsl.FlowWithContext[ByteString, Ctx, ParseEvent, Ctx, NotUsed] =
    xml.scaladsl.XmlParsing.parserWithContext(ignoreInvalidChars, configureFactory.accept(_)).asJava

  /**
   * A Flow that transforms a stream of XML ParseEvents. This stage coalesces consequitive CData and Characters
   * events into a single Characters event or fails if the buffered string is larger than the maximum defined.
   */
  def coalesce(maximumTextLength: Int): akka.stream.javadsl.Flow[ParseEvent, ParseEvent, NotUsed] =
    xml.scaladsl.XmlParsing.coalesce(maximumTextLength).asJava

  /**
   * A Flow that transforms a stream of XML ParseEvents. This stage filters out any event not corresponding to
   * a certain path in the XML document. Any event that is under the specified path (including subpaths) is passed
   * through.
   */
  def subslice(path: java.util.Collection[String]): akka.stream.javadsl.Flow[ParseEvent, ParseEvent, NotUsed] =
    xml.scaladsl.XmlParsing.subslice(path.asScala.toIndexedSeq).asJava

  /**
   * A Flow that transforms a stream of XML ParseEvents. This stage pushes elements of a certain path in
   * the XML document as org.w3c.dom.Element.
   */
  def subtree(path: java.util.Collection[String]): akka.stream.javadsl.Flow[ParseEvent, Element, NotUsed] =
    xml.scaladsl.XmlParsing.subtree(path.asScala.toIndexedSeq).asJava
}
