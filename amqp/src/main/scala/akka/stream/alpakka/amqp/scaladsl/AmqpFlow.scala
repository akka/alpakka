/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.scaladsl

import akka.stream.alpakka.amqp._
import akka.stream.scaladsl.{Flow, Keep}
import akka.{Done, NotUsed}

import scala.concurrent.Future

object AmqpFlow {

  /**
   * Creates an `AmqpFlow` that accepts `WriteMessage` elements and emits `WriteResult`.
   *
   * This variant of `AmqpFlow` publishes messages in a fire-and-forget manner, hence all emitted `WriteResult`s
   * have `confirmed` flag set to `true`.
   *
   * This stage materializes to a `Future` of `Done`, which can be used to know when the Flow completes,
   * either normally or because of an amqp failure.
   *
   * @param settings `bufferSize` and `confirmationTimeout` properties are ignored by this connector
   */
  def apply(
      settings: AmqpWriteSettings
  ): Flow[WriteMessage, WriteResult, Future[Done]] =
    asFlowWithoutContext(
      Flow.fromGraph(new impl.AmqpSimpleFlowStage(settings))
    )

  /**
   * Creates an `AmqpFlow` that accepts `WriteMessage` elements and emits `WriteResult`.
   *
   * This variant of `AmqpFlow` asynchronously waits for message confirmations. Maximum number of messages
   * simultaneously waiting for confirmation before signaling backpressure is configured with a
   * `bufferSize` parameter. Emitted results preserve the order of messages pulled from upstream - due to that
   * restriction this flow is expected to be slightly less effective than it's unordered counterpart.
   *
   * In case of upstream failure/finish this stage attempts to process all buffered messages (waiting for
   * confirmation) before propagating failure/finish downstream.
   *
   * This stage materializes to a `Future` of `Done`, which can be used to know when the Flow completes,
   * either normally or because of an amqp failure.
   */
  def withConfirm(
      settings: AmqpWriteSettings
  ): Flow[WriteMessage, WriteResult, Future[Done]] =
    asFlowWithoutContext(
      Flow.fromGraph(new impl.AmqpAsyncFlowStage(settings))
    )

  /**
   * Creates an `AmqpFlow` that accepts `WriteMessage` elements and emits `WriteResult`.
   *
   * This variant of `AmqpFlow` asynchronously waits for message confirmations. Maximum number of messages
   * simultaneously waiting for confirmation before signaling backpressure is configured with a
   * `bufferSize` parameter. Results are emitted downstream as soon as confirmation is received, meaning that
   * there is no ordering guarantee of any sort.
   *
   * In case of upstream failure/finish this stage attempts to process all buffered messages (waiting for
   * confirmation) before propagating failure/finish downstream.
   *
   * This stage materializes to a `Future` of `Done`, which can be used to know when the Flow completes,
   * either normally or because of an amqp failure.
   *
   * NOTE: This connector uses RabbitMQ's extension to AMQP protocol
   * ([[https://www.rabbitmq.com/confirms.html#publisher-confirms Publisher Confirms]]), therefore it is not
   * supposed to be used with another AMQP brokers.
   */
  def withConfirmUnordered(
      settings: AmqpWriteSettings
  ): Flow[WriteMessage, WriteResult, Future[Done]] =
    asFlowWithoutContext(
      Flow.fromGraph(new impl.AmqpAsyncUnorderedFlowStage(settings))
    )

  /**
   * Variant of `AmqpFlow.withConfirmUnordered` with additional support for pass-through elements.
   *
   * @see [[AmqpFlow.withConfirmUnordered]]
   *
   * NOTE: This connector uses RabbitMQ's extension to AMQP protocol
   * ([[https://www.rabbitmq.com/confirms.html#publisher-confirms Publisher Confirms]]), therefore it is not
   * supposed to be used with another AMQP brokers.
   */
  def withConfirmAndPassThroughUnordered[T](
      settings: AmqpWriteSettings
  ): Flow[(WriteMessage, T), (WriteResult, T), Future[Done]] =
    Flow.fromGraph(new impl.AmqpAsyncUnorderedFlowStage(settings))

  private def asFlowWithoutContext(flow: Flow[(WriteMessage, NotUsed), (WriteResult, NotUsed), Future[Done]]) =
    Flow[WriteMessage]
      .map(message => (message, NotUsed))
      .viaMat(flow)(Keep.right)
      .map { case (message, _) => message }
}
