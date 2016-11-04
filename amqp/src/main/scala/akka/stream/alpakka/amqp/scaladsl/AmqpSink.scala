/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.amqp.scaladsl

import akka.NotUsed
import akka.stream.alpakka.amqp.{ AmqpSinkStage, AmqpSinkSettings, OutgoingMessage }
import akka.stream.scaladsl.Sink
import akka.util.ByteString

object AmqpSink {
  /**
   * Scala API: Creates an [[AmqpSink]] that accepts ByteString elements.
   */
  def simple(settings: AmqpSinkSettings): Sink[ByteString, NotUsed] =
    apply(settings).contramap[ByteString](bytes => OutgoingMessage(bytes, false, false, None))

  /**
   * Scala API: Creates an [[AmqpSink]] that accepts [[OutgoingMessage]] elements.
   */
  def apply(settings: AmqpSinkSettings): Sink[OutgoingMessage, NotUsed] =
    Sink.fromGraph(new AmqpSinkStage(settings))

}
