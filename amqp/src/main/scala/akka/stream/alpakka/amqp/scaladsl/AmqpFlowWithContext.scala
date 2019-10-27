/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.scaladsl

import akka.Done
import akka.stream.alpakka.amqp._
import akka.stream.scaladsl.{Flow, FlowWithContext}

import scala.concurrent.Future
import scala.concurrent.duration._

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
   */
  def withConfirm[T](
      settings: AmqpWriteSettings,
      confirmationTimeout: FiniteDuration
  ): FlowWithContext[WriteMessage, T, WriteResult, T, Future[Done]] =
    FlowWithContext.fromTuples(
      Flow.fromGraph(new impl.AmqpBlockingFlowStage[T](settings, confirmationTimeout))
    )

  /**
   * Creates a contextual variant of corresponding [[AmqpFlow]].
   *
   * @see [[AmqpFlow.withAsyncConfirm]]
   */
  def withAsyncConfirm[T](
      settings: AmqpWriteSettings,
      bufferSize: Int,
      confirmationTimeout: FiniteDuration
  ): FlowWithContext[WriteMessage, T, WriteResult, T, Future[Done]] =
    FlowWithContext.fromTuples(
      Flow.fromGraph(new impl.AmqpAsyncFlowStage(settings, bufferSize, confirmationTimeout))
    )

  /**
   * Creates a contextual variant of corresponding [[AmqpFlow]].
   *
   * @see [[AmqpFlow.withAsyncUnorderedConfirm]]
   */
  def withAsyncUnorderedConfirm[T](
      settings: AmqpWriteSettings,
      bufferSize: Int,
      confirmationTimeout: FiniteDuration
  ): FlowWithContext[WriteMessage, T, WriteResult, T, Future[Done]] =
    FlowWithContext.fromTuples(
      Flow.fromGraph(new impl.AmqpAsyncUnorderedFlowStage(settings, bufferSize, confirmationTimeout))
    )
}
