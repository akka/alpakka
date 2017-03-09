/*
 * Copyright (C) 2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.amqp.javadsl

import akka.NotUsed
import akka.stream.alpakka.amqp.{AmqpSinkSettings, AmqpSinkStage, OutgoingMessage}
import akka.stream.javadsl.Sink
import akka.util.ByteString

object AmqpSink {

  /**
   * Java API: Creates an [[AmqpSink]] that accepts [[OutgoingMessage]] elements.
   */
  def create(settings: AmqpSinkSettings): akka.stream.javadsl.Sink[OutgoingMessage, NotUsed] =
    Sink.fromGraph(new AmqpSinkStage(settings))

  /**
   * Java API: Creates an [[AmqpSink]] that accepts ByteString elements.
   */
  def createSimple(settings: AmqpSinkSettings): akka.stream.javadsl.Sink[ByteString, NotUsed] =
    akka.stream.alpakka.amqp.scaladsl.AmqpSink.simple(settings).asJava

}
