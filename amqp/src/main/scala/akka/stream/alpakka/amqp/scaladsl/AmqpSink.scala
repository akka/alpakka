/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.amqp.scaladsl

import akka.Done
import akka.stream.alpakka.amqp._
import akka.stream.scaladsl.Sink
import akka.util.ByteString

import scala.concurrent.Future

object AmqpSink {

  /**
   * Scala API: Creates an [[AmqpSink]] that accepts ByteString elements.
   *
   * This stage materializes to a `Future[Done]`, which can be used to know when the Sink completes, either normally
   * or because of an amqp failure
   */
  def simple(settings: AmqpSinkSettings): Sink[ByteString, Future[Done]] =
    apply(settings).contramap[ByteString](bytes => OutgoingMessage(bytes, false, false, None))

  /**
   * Scala API:
   *
   * Connects to an AMQP server upon materialization and sends incoming messages to the server.
   * Each materialized sink will create one connection to the broker. This stage sends messages to
   * the queue named in the replyTo options of the message instead of from settings declared at construction.
   *
   * This stage materializes to a `Future[Done]`, which can be used to know when the Sink completes, either normally
   * or because of an amqp failure
   */
  def replyTo(settings: AmqpReplyToSinkSettings): Sink[OutgoingMessage, Future[Done]] =
    Sink.fromGraph(new AmqpReplyToSinkStage(settings))

  /**
   * Scala API:
   *
   * Connects to an AMQP server upon materialization and sends incoming messages to the server.
   * Each materialized sink will create one connection to the broker. This stage sends messages to
   * the queue named in the replyTo options of the message instead of from settings declared at construction.
   *
   * This stage materializes to a Future[Done], which can be used to know when the Sink completes, either normally
   * or because of an amqp failure
   */
  def apply(settings: AmqpSinkSettings): Sink[OutgoingMessage, Future[Done]] =
    Sink.fromGraph(new AmqpSinkStage(settings))

}
