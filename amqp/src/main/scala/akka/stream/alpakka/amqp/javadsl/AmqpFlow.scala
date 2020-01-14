/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.japi.Pair
import akka.stream.alpakka.amqp._
import akka.stream.scaladsl.Keep

import scala.compat.java8.FutureConverters._

object AmqpFlow {

  /**
   * Creates an `AmqpFlow` that accepts `WriteMessage` elements and emits `WriteResult`.
   *
   * This variant of `AmqpFlow` publishes messages in a fire-and-forget manner, hence all emitted `WriteResult`s
   * have `confirmed` flag set to `true`.
   *
   * This stage materializes to a `CompletionStage` of `Done`, which can be used to know when the Flow completes,
   * either normally or because of an amqp failure.
   *
   * @param settings `bufferSize` and `confirmationTimeout` properties are ignored by this connector
   */
  def create(
      settings: AmqpWriteSettings
  ): akka.stream.javadsl.Flow[WriteMessage, WriteResult, CompletionStage[Done]] =
    akka.stream.alpakka.amqp.scaladsl.AmqpFlow(settings).mapMaterializedValue(f => f.toJava).asJava

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
   * This stage materializes to a `CompletionStage` of `Done`, which can be used to know when the Flow completes,
   * either normally or because of an amqp failure.
   *
   * NOTE: This connector uses RabbitMQ's extension to AMQP protocol
   * ([[https://www.rabbitmq.com/confirms.html#publisher-confirms Publisher Confirms]]), therefore it is not
   * supposed to be used with another AMQP brokers.
   */
  def createWithConfirm(
      settings: AmqpWriteSettings
  ): akka.stream.javadsl.Flow[WriteMessage, WriteResult, CompletionStage[Done]] =
    akka.stream.alpakka.amqp.scaladsl.AmqpFlow
      .withConfirm(settings = settings)
      .mapMaterializedValue(_.toJava)
      .asJava

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
   * This stage materializes to a `CompletionStage` of `Done`, which can be used to know when the Flow completes,
   * either normally or because of an amqp failure.
   *
   * NOTE: This connector uses RabbitMQ's extension to AMQP protocol
   * ([[https://www.rabbitmq.com/confirms.html#publisher-confirms Publisher Confirms]]), therefore it is not
   * supposed to be used with another AMQP brokers.
   */
  def createWithConfirmUnordered(
      settings: AmqpWriteSettings
  ): akka.stream.javadsl.Flow[WriteMessage, WriteResult, CompletionStage[Done]] =
    akka.stream.alpakka.amqp.scaladsl.AmqpFlow
      .withConfirmUnordered(settings)
      .mapMaterializedValue(_.toJava)
      .asJava

  /**
   * Variant of `AmqpFlow.createWithConfirmUnordered` with additional support for pass-through elements.
   *
   * @see [[AmqpFlow.createWithConfirmUnordered]]
   *
   * NOTE: This connector uses RabbitMQ's extension to AMQP protocol
   * ([[https://www.rabbitmq.com/confirms.html#publisher-confirms Publisher Confirms]]), therefore it is not
   * supposed to be used with another AMQP brokers.
   */
  def createWithConfirmAndPassThroughUnordered[T](
      settings: AmqpWriteSettings
  ): akka.stream.javadsl.Flow[Pair[WriteMessage, T], Pair[WriteResult, T], CompletionStage[Done]] =
    akka.stream.scaladsl
      .Flow[Pair[WriteMessage, T]]
      .map((p: Pair[WriteMessage, T]) => p.toScala)
      .viaMat(
        akka.stream.alpakka.amqp.scaladsl.AmqpFlow
          .withConfirmAndPassThroughUnordered[T](settings = settings)
      )(Keep.right)
      .map { case (writeResult, passThrough) => Pair(writeResult, passThrough) }
      .mapMaterializedValue(_.toJava)
      .asJava
}
