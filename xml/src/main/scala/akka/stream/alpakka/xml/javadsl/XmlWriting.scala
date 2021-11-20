/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.xml.javadsl

import java.nio.charset.{Charset, StandardCharsets}

import akka.NotUsed
import akka.stream.alpakka.xml.ParseEvent
import akka.stream.alpakka.xml.impl
import akka.stream.scaladsl.Flow
import akka.util.ByteString

import javax.xml.stream.XMLOutputFactory

object XmlWriting {

  /**
   * Writer Flow that takes a stream of XML events similar to SAX and write ByteStrings.
   * encoding UTF-8
   */
  def writer(): akka.stream.javadsl.Flow[ParseEvent, ByteString, NotUsed] =
    Flow.fromGraph(new impl.StreamingXmlWriter(StandardCharsets.UTF_8)).asJava

  /**
   * Writer Flow that takes a stream of XML events similar to SAX and write ByteStrings.
   * @param charset encoding of the stream
   */
  def writer(charset: Charset): akka.stream.javadsl.Flow[ParseEvent, ByteString, NotUsed] =
    Flow.fromGraph(new impl.StreamingXmlWriter(charset)).asJava

  /**
   * Writer Flow that takes a stream of XML events similar to SAX and write ByteStrings.
   * @param xmlOutputFactory factory from which to get an XMLStreamWriter
   */
  def writer(xmlOutputFactory: XMLOutputFactory): akka.stream.javadsl.Flow[ParseEvent, ByteString, NotUsed] =
    Flow.fromGraph(new impl.StreamingXmlWriter(StandardCharsets.UTF_8, xmlOutputFactory)).asJava

  /**
   * Writer Flow that takes a stream of XML events similar to SAX and write ByteStrings.
   * @param charset encoding of the stream
   * @param xmlOutputFactory factory from which to get an XMLStreamWriter
   */
  def writer(charset: Charset,
             xmlOutputFactory: XMLOutputFactory): akka.stream.javadsl.Flow[ParseEvent, ByteString, NotUsed] =
    Flow.fromGraph(new impl.StreamingXmlWriter(charset, xmlOutputFactory)).asJava

}
