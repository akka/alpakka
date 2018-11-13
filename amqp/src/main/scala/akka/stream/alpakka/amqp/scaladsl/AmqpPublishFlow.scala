/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.scaladsl

import akka.Done
import akka.stream.alpakka.amqp.{AmqpSinkSettings, OutgoingMessage}
import akka.stream.alpakka.amqp.impl.AmqpPublishFlowStage
import akka.stream.scaladsl.{Flow, Keep}
import akka.util.ByteString

import scala.concurrent.Future

object AmqpPublishFlow {

  /**
   * Scala API: Creates an [[AmqpPublishFlow]] that accepts (ByteString, passthrough) elements.
   *
   * This stage materializes to a `Future[Done]`, which can be used to know when the Flow completes, either normally
   * or because of an amqp failure
   */
  def simple[O](settings: AmqpSinkSettings): Flow[(ByteString, O), O, Future[Done]] =
    Flow[(ByteString, O)]
      .map { case (s, passthrough) => (OutgoingMessage(s, false, false), passthrough) }
      .viaMat(apply[O](settings))(Keep.right)

  /**
   * Scala API:
   *
   * Connects to an AMQP server upon materialization and sends incoming messages to the server.
   * Each materialized flow will create one connection to the broker. This stage sends messages to
   * the queue named in the replyTo options of the message instead of from settings declared at construction.
   *
   * This stage materializes to a Future[Done], which can be used to know when the Flow completes, either normally
   * or because of an amqp failure
   */
  def apply[O](settings: AmqpSinkSettings): Flow[(OutgoingMessage, O), O, Future[Done]] =
    Flow.fromGraph(new AmqpPublishFlowStage[O](settings))
}
