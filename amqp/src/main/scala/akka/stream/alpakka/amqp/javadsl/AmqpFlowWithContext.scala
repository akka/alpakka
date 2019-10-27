/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.amqp._
import akka.util.JavaDurationConverters

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
    akka.stream.scaladsl.FlowWithContext
      .fromTuples(
        akka.stream.scaladsl.Flow
          .fromGraph(new impl.AmqpSimpleFlowStage[T](settings))
          .mapMaterializedValue(f => f.toJava)
      )
      .asJava

  /**
   * Creates a contextual variant of corresponding [[AmqpFlow]].
   *
   * @see [[AmqpFlow.createWithConfirm]]
   */
  def createWithConfirm[T](
      settings: AmqpWriteSettings,
      confirmationTimeout: java.time.Duration
  ): akka.stream.javadsl.FlowWithContext[WriteMessage, T, WriteResult, T, CompletionStage[Done]] =
    akka.stream.scaladsl.FlowWithContext
      .fromTuples(
        akka.stream.scaladsl.Flow
          .fromGraph(
            new impl.AmqpBlockingFlowStage[T](
              settings = settings,
              confirmationTimeout = JavaDurationConverters.asFiniteDuration(confirmationTimeout)
            )
          )
          .mapMaterializedValue(f => f.toJava)
      )
      .asJava

  /**
   * Creates a contextual variant of corresponding [[AmqpFlow]].
   *
   * @see [[AmqpFlow.createWithAsyncConfirm]]
   */
  def createWithAsyncConfirm[T](
      settings: AmqpWriteSettings,
      bufferSize: Int,
      confirmationTimeout: java.time.Duration
  ): akka.stream.javadsl.FlowWithContext[WriteMessage, T, WriteResult, T, CompletionStage[Done]] =
    akka.stream.scaladsl.FlowWithContext
      .fromTuples(
        akka.stream.scaladsl.Flow
          .fromGraph(
            new impl.AmqpAsyncFlowStage[T](
              settings = settings,
              bufferSize = bufferSize,
              confirmationTimeout = JavaDurationConverters.asFiniteDuration(confirmationTimeout)
            )
          )
          .mapMaterializedValue(f => f.toJava)
      )
      .asJava

  /**
   * Creates a contextual variant of corresponding [[AmqpFlow]].
   *
   * @see [[AmqpFlow.createWithAsyncUnorderedConfirm]]
   */
  def createWithAsyncUnorderedConfirm[T](
      settings: AmqpWriteSettings,
      bufferSize: Int,
      confirmationTimeout: java.time.Duration
  ): akka.stream.javadsl.FlowWithContext[WriteMessage, T, WriteResult, T, CompletionStage[Done]] =
    akka.stream.scaladsl.FlowWithContext
      .fromTuples(
        akka.stream.scaladsl.Flow
          .fromGraph(
            new impl.AmqpAsyncUnorderedFlowStage[T](
              settings = settings,
              bufferSize = bufferSize,
              confirmationTimeout = JavaDurationConverters.asFiniteDuration(confirmationTimeout)
            )
          )
          .mapMaterializedValue(f => f.toJava)
      )
      .asJava
}
