/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.amqp._
import akka.util.ByteString

import scala.compat.java8.FutureConverters._

object AmqpSink {

  /**
   * Creates an `AmqpSink` that accepts `WriteMessage` elements.
   *
   * This stage materializes to a `CompletionStage` of `Done`, which can be used to know when the Sink completes,
   * either normally or because of an amqp failure.
   */
  def create(settings: AmqpWriteSettings): akka.stream.javadsl.Sink[WriteMessage, CompletionStage[Done]] =
    akka.stream.alpakka.amqp.scaladsl.AmqpSink(settings).mapMaterializedValue(f => f.toJava).asJava

  /**
   * Creates an `AmqpSink` that accepts `ByteString` elements.
   *
   * This stage materializes to a `CompletionStage` of `Done`, which can be used to know when the Sink completes,
   * either normally or because of an amqp failure.
   */
  def createSimple(settings: AmqpWriteSettings): akka.stream.javadsl.Sink[ByteString, CompletionStage[Done]] =
    akka.stream.alpakka.amqp.scaladsl.AmqpSink.simple(settings).mapMaterializedValue(f => f.toJava).asJava

  /**
   * Connects to an AMQP server upon materialization and sends incoming messages to the server.
   * Each materialized sink will create one connection to the broker. This stage sends messages to
   * the queue named in the replyTo options of the message instead of from settings declared at construction.
   *
   * This stage materializes to a `CompletionStage` of `Done`, which can be used to know when the Sink completes,
   * either normally or because of an amqp failure.
   */
  def createReplyTo(
      settings: AmqpReplyToSinkSettings
  ): akka.stream.javadsl.Sink[WriteMessage, CompletionStage[Done]] =
    akka.stream.alpakka.amqp.scaladsl.AmqpSink.replyTo(settings).mapMaterializedValue(f => f.toJava).asJava

}
