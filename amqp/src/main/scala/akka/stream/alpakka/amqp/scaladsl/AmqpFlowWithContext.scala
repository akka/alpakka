/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.scaladsl

import akka.Done
import akka.stream.alpakka.amqp._
import akka.stream.scaladsl.{Flow, FlowWithContext}

import scala.concurrent.Future

object AmqpFlowWithContext {

  /**
   * Creates a contextual variant of corresponding [[AmqpFlow]].
   *
   * @see [[AmqpFlow.apply]]
   */
  def apply[T](
      settings: AmqpWriteSettings
  ): FlowWithContext[WriteMessage, T, WriteResult, T, Future[Done]] =
    FlowWithContext.fromTuples(
      Flow.fromGraph(new impl.AmqpSimpleFlowStage[T](settings))
    )

  /**
   * Creates a contextual variant of corresponding [[AmqpFlow]].
   *
   * @see [[AmqpFlow.withConfirm]]
   *
   * NOTE: This connector uses RabbitMQ's extension to AMQP protocol
   * ([[https://www.rabbitmq.com/confirms.html#publisher-confirms Publisher Confirms]]), therefore it is not
   * supposed to be used with another AMQP brokers.
   */
  def withConfirm[T](
      settings: AmqpWriteSettings
  ): FlowWithContext[WriteMessage, T, WriteResult, T, Future[Done]] =
    FlowWithContext.fromTuples(
      Flow.fromGraph(new impl.AmqpAsyncFlowStage(settings))
    )
}
