/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.xml.javadsl

import java.nio.charset.{Charset, StandardCharsets}

import akka.NotUsed
import akka.stream.alpakka.xml.ParseEvent
import akka.stream.alpakka.xml.Xml.StreamingXmlWriter
import akka.stream.scaladsl.Flow
import akka.util.ByteString

object XmlWriting {

  /**
   * Writer Flow that takes a stream of XML events similar to SAX and write ByteStrings.
   * encoding UTF-8
   */
  def writer(): akka.stream.javadsl.Flow[ParseEvent, ByteString, NotUsed] =
    Flow.fromGraph(new StreamingXmlWriter(StandardCharsets.UTF_8)).asJava

  /**
   * Writer Flow that takes a stream of XML events similar to SAX and write ByteStrings.
   * @param strEncoding encoding of the stream
   */
  def writer(charset: Charset): akka.stream.javadsl.Flow[ParseEvent, ByteString, NotUsed] =
    Flow.fromGraph(new StreamingXmlWriter(charset)).asJava

}
