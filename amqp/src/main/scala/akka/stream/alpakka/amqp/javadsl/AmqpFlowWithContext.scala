/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.amqp._

import scala.compat.java8.FutureConverters._

object AmqpFlowWithContext {

  /**
   * Creates a contextual variant of corresponding [[AmqpFlow]].
   *
   * @see [[AmqpFlow.create]]
   */
  def create[T](
      settings: AmqpWriteSettings
  ): akka.stream.javadsl.FlowWithContext[WriteMessage, T, WriteResult, T, CompletionStage[Done]] =
    akka.stream.alpakka.amqp.scaladsl.AmqpFlowWithContext
      .apply(settings)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Creates a contextual variant of corresponding [[AmqpFlow]].
   *
   * @see [[AmqpFlow.createWithConfirm]]
   *
   * NOTE: This connector uses RabbitMQ's extension to AMQP protocol
   * ([[https://www.rabbitmq.com/confirms.html#publisher-confirms Publisher Confirms]]), therefore it is not
   * supposed to be used with another AMQP brokers.
   */
  def createWithConfirm[T](
      settings: AmqpWriteSettings
  ): akka.stream.javadsl.FlowWithContext[WriteMessage, T, WriteResult, T, CompletionStage[Done]] =
    akka.stream.alpakka.amqp.scaladsl.AmqpFlowWithContext
      .withConfirm(settings)
      .mapMaterializedValue(_.toJava)
      .asJava
}
