/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.xml.scaladsl

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
   * @param charset charset of encoding
   */
  def writer(charset: Charset): Flow[ParseEvent, ByteString, NotUsed] =
    Flow.fromGraph(new impl.StreamingXmlWriter(charset))

  /**
   * Writer Flow that takes a stream of XML events similar to SAX and write ByteStrings.
   * encoding UTF-8
   * @param xmlOutputFactory factory from which to get an XMLStreamWriter
   */
  def writer(xmlOutputFactory: XMLOutputFactory): Flow[ParseEvent, ByteString, NotUsed] =
    Flow.fromGraph(new impl.StreamingXmlWriter(StandardCharsets.UTF_8, xmlOutputFactory))

  /**
   * Writer Flow that takes a stream of XML events similar to SAX and write ByteStrings.
   * @param charset charset of encoding
   * @param xmlOutputFactory factory from which to get an XMLStreamWriter
   */
  def writer(charset: Charset, xmlOutputFactory: XMLOutputFactory): Flow[ParseEvent, ByteString, NotUsed] =
    Flow.fromGraph(new impl.StreamingXmlWriter(charset, xmlOutputFactory))

  /**
   * Writer Flow that takes a stream of XML events similar to SAX and write ByteStrings.
   * encoding UTF-8
   */
  val writer: Flow[ParseEvent, ByteString, NotUsed] = writer(StandardCharsets.UTF_8)
}
