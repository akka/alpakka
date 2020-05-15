/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.text.scaladsl

import java.nio.charset.{Charset, StandardCharsets}

import akka.NotUsed
import akka.stream.alpakka.text.impl.{CharsetCanonicalizingFlow, CharsetDecodingFlow, CharsetTranscodingFlow}
import akka.stream.scaladsl.Flow
import akka.util.ByteString

/**
 * Scala DSL
 */
object TextFlow {

  /**
   * Decodes a stream of bytes into a stream of characters, using the supplied charset.
   */
  def decoding(incoming: Charset): Flow[ByteString, String, NotUsed] =
    Flow[ByteString]
      .via(new CharsetDecodingFlow(incoming))

  /**
   * Decodes a stream of bytes into a stream of characters, using the supplied charset.
   */
  def encoding(outgoing: Charset): Flow[String, ByteString, NotUsed] =
    Flow[String]
      .map(ByteString(_, outgoing))

  /**
   * Translates a stream of bytes from one character encoding into another.
   */
  def transcoding(incoming: Charset, outgoing: Charset): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString]
      .via(new CharsetTranscodingFlow(incoming, outgoing))

  /**
   * Do charset detection on the incoming stream and translate it to the target charset.
   */
  def canonicalizing(outgoing: Charset = StandardCharsets.UTF_8): Flow[ByteString, ByteString, NotUsed] =
    new CharsetCanonicalizingFlow(outgoing).flow()
}
