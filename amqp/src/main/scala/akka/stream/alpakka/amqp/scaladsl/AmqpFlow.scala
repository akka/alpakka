/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.scaladsl

import akka.Done
import akka.stream.alpakka.amqp._
import akka.stream.scaladsl.Flow

import scala.concurrent.Future
import scala.concurrent.duration._

object AmqpFlow {

  /**
   * Creates an [[AmqpFlow]] that accepts [[WriteMessage]] elements and emits [[WriteResult]].
   *
   * This variant of [[AmqpFlow]] publishes messages in a fire-and-forget manner, hence all emitted [[WriteResult]]s
   * have `confirmed` flag set to `true`.
   *
   * This stage materializes to a [[Future]] of [[Done]], which can be used to know when the Flow completes,
   * either normally or because of an amqp failure.
   */
  def apply[T](
      settings: AmqpWriteSettings
  ): Flow[WriteMessage[T], WriteResult[T], Future[Done]] =
    Flow.fromGraph(
      new impl.AmqpSimpleFlow[T](settings)
    )

  /**
   * Creates an [[AmqpFlow]] that accepts [[WriteMessage]] elements and emits [[WriteResult]].
   *
   * This variant of [[AmqpFlow]] waits for confirmation after every single message publication.
   * It can be used to ensure order of messages accepted by queue at the cost of significantly lower publication
   * throughput. Please note that such strict ordering guarantee is rarely needed, and in most cases it's perfectly
   * sufficient to use `createWithAsyncConfirm` or `createWithAsyncUnorderedConfirm` for better performance.
   *
   * This stage materializes to a [[Future]] of [[Done]], which can be used to know when the Flow completes,
   * either normally or because of an amqp failure.
   */
  def withConfirm[T](
      settings: AmqpWriteSettings,
      confirmationTimeout: FiniteDuration
  ): Flow[WriteMessage[T], WriteResult[T], Future[Done]] =
    Flow.fromGraph(
      new impl.AmqpBlockingFlow[T](settings, confirmationTimeout)
    )

  /**
   * Creates an [[AmqpFlow]] that accepts [[WriteMessage]] elements and emits [[WriteResult]].
   *
   * This variant of [[AmqpFlow]] asynchronously waits for message confirmations. Maximum number of messages
   * simultaneously waiting for confirmation before signaling backpressure is configured with a
   * `bufferSize` parameter. Emitted results preserve the order of messages pulled from upstream - due to that
   * restriction this flow is expected to be slightly less effective than it's unordered counterpart.
   *
   * In case of upstream failure/finish this stage attempts to process all buffered messages (waiting for
   * confirmation) before propagating failure/finish downstream.
   *
   * This stage materializes to a [[Future]] of [[Done]], which can be used to know when the Flow completes,
   * either normally or because of an amqp failure.
   */
  def withAsyncConfirm[T](
      settings: AmqpWriteSettings,
      bufferSize: Int,
      confirmationTimeout: FiniteDuration
  ): Flow[WriteMessage[T], WriteResult[T], Future[Done]] =
    Flow.fromGraph(
      new impl.AmqpAsyncFlow(settings, bufferSize, confirmationTimeout)
    )

  /**
   * Creates an [[AmqpFlow]] that accepts [[WriteMessage]] elements and emits [[WriteResult]].
   *
   * This variant of [[AmqpFlow]] asynchronously waits for message confirmations. Maximum number of messages
   * simultaneously waiting for confirmation before signaling backpressure is configured with a
   * `bufferSize` parameter. Results are emitted downstream as soon as confirmation is received, meaning that
   * there is no ordering guarantee of any sort.
   *
   * In case of upstream failure/finish this stage attempts to process all buffered messages (waiting for
   * confirmation) before propagating failure/finish downstream.
   *
   * This stage materializes to a [[Future]] of [[Done]], which can be used to know when the Flow completes,
   * either normally or because of an amqp failure.
   */
  def withAsyncUnorderedConfirm[T](
      settings: AmqpWriteSettings,
      bufferSize: Int,
      confirmationTimeout: FiniteDuration
  ): Flow[WriteMessage[T], WriteResult[T], Future[Done]] =
    Flow.fromGraph(
      new impl.AmqpAsyncUnorderedFlow(settings, bufferSize, confirmationTimeout)
    )
}
