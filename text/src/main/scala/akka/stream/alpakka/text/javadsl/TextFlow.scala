/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.text.javadsl

import java.nio.charset.Charset

import akka.NotUsed
import akka.stream.alpakka.text.impl.{CharsetDecodingFlow, CharsetTranscodingFlow}
import akka.stream.javadsl.Flow
import akka.util.ByteString

/**
 * Java DSL
 */
object TextFlow {

  /**
   * Decodes a stream of bytes into a stream of characters, using the supplied charset.
   */
  def decoding(incoming: Charset): Flow[ByteString, String, NotUsed] =
    akka.stream.scaladsl
      .Flow[ByteString]
      .via(new CharsetDecodingFlow(incoming))
      .asJava

  /**
   * Decodes a stream of bytes into a stream of characters, using the supplied charset.
   */
  def encoding(outgoing: Charset): Flow[String, ByteString, NotUsed] =
    Flow.fromFunction((s: String) => ByteString.fromString(s, outgoing))

  /**
   * Translates a stream of bytes from one character encoding into another.
   */
  def transcoding(incoming: Charset, outgoing: Charset): Flow[ByteString, ByteString, NotUsed] =
    akka.stream.scaladsl
      .Flow[ByteString]
      .via(new CharsetTranscodingFlow(incoming, outgoing))
      .asJava

}
