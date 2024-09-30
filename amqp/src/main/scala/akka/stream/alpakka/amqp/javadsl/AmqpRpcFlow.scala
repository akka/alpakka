/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.javadsl

import java.util.concurrent.CompletionStage

import akka.stream.alpakka.amqp._
import akka.stream.javadsl.Flow
import akka.util.ByteString

import scala.jdk.FutureConverters._

object AmqpRpcFlow {

  /**
   * Java API:
   * Create an [[https://www.rabbitmq.com/tutorials/tutorial-six-java.html RPC style flow]] for processing and communicating
   * over a rabbitmq message bus. This will create a private queue, and add the reply-to header to messages sent out.
   *
   * This stage materializes to a `CompletionStage<String>`, which is the name of the private exclusive queue used for RPC communication.
   *
   * @param repliesPerMessage The number of responses that should be expected for each message placed on the queue.
   */
  def createSimple(settings: AmqpWriteSettings,
                   repliesPerMessage: Int): Flow[ByteString, ByteString, CompletionStage[String]] =
    akka.stream.alpakka.amqp.scaladsl.AmqpRpcFlow
      .simple(settings, repliesPerMessage)
      .mapMaterializedValue(f => f.asJava)
      .asJava

  /**
   * Java API:
   * Convenience for "at-most once delivery" semantics. Each message is acked to RabbitMQ
   * before its read result is emitted downstream.
   */
  def atMostOnceFlow(settings: AmqpWriteSettings,
                     bufferSize: Int): Flow[WriteMessage, ReadResult, CompletionStage[String]] =
    akka.stream.alpakka.amqp.scaladsl.AmqpRpcFlow
      .atMostOnceFlow(settings, bufferSize)
      .mapMaterializedValue(f => f.asJava)
      .asJava

  /**
   * Java API:
   * Convenience for "at-most once delivery" semantics. Each message is acked to RabbitMQ
   * before its read result is emitted downstream.
   */
  def atMostOnceFlow(settings: AmqpWriteSettings,
                     bufferSize: Int,
                     repliesPerMessage: Int): Flow[WriteMessage, ReadResult, CompletionStage[String]] =
    akka.stream.alpakka.amqp.scaladsl.AmqpRpcFlow
      .atMostOnceFlow(settings, bufferSize, repliesPerMessage)
      .mapMaterializedValue(f => f.asJava)
      .asJava

  /**
   * Java API:
   * The `committableFlow` makes it possible to commit (ack/nack) messages to RabbitMQ.
   * This is useful when "at-least once delivery" is desired, as each message will likely be
   * delivered one time but in failure cases could be duplicated.
   *
   * If you commit the offset before processing the message you get "at-most once delivery" semantics,
   * and for that there is a [[#atMostOnceFlow]].
   *
   * Compared to auto-commit, this gives exact control over when a message is considered consumed.
   */
  def committableFlow(
      settings: AmqpWriteSettings,
      bufferSize: Int,
      repliesPerMessage: Int = 1
  ): Flow[WriteMessage, CommittableReadResult, CompletionStage[String]] =
    akka.stream.alpakka.amqp.scaladsl.AmqpRpcFlow
      .committableFlow(settings, bufferSize, repliesPerMessage)
      .mapMaterializedValue(f => f.asJava)
      .map(cm => new CommittableReadResult(cm))
      .asJava

}
