/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.scaladsl

import akka.NotUsed
import akka.stream.alpakka.amqp.{AmqpWriteSettings, WriteMessage}
import akka.stream.alpakka.amqp.impl.AmqpPublishFlowStage
import akka.stream.scaladsl.{Flow, Keep}
import akka.util.ByteString

object AmqpFlow {

  /**
   * Scala API: Creates an [[AmqpFlow]] that accepts (ByteString, passthrough) elements.
   *
   * This stage materializes to a `Future[Done]`, which can be used to know when the Flow completes, either normally
   * or because of an amqp failure
   */
  def simple[O](settings: AmqpWriteSettings): Flow[(ByteString, O), O, NotUsed] =
    Flow[(ByteString, O)]
      .map { case (s, passthrough) => (WriteMessage(s, false, false), passthrough) }
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
  def apply[O](settings: AmqpWriteSettings): Flow[(WriteMessage, O), O, NotUsed] =
    Flow.fromGraph(new AmqpPublishFlowStage[O](settings))
}
