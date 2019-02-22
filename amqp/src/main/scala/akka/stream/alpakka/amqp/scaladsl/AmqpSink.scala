/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.scaladsl

import akka.Done
import akka.annotation.ApiMayChange
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
  def simple(settings: AmqpWriteSettings): Sink[ByteString, Future[Done]] =
    apply(settings).contramap[ByteString](bytes => WriteMessage(bytes))

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
  @ApiMayChange // https://github.com/akka/alpakka/issues/1513
  def replyTo(settings: AmqpReplyToSinkSettings): Sink[WriteMessage, Future[Done]] =
    Sink.fromGraph(new impl.AmqpReplyToSinkStage(settings))

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
  @ApiMayChange // https://github.com/akka/alpakka/issues/1513
  def apply(settings: AmqpWriteSettings): Sink[WriteMessage, Future[Done]] =
    Sink.fromGraph(new impl.AmqpSinkStage(settings))

}
