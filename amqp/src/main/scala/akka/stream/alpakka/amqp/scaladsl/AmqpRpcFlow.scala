/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.scaladsl

import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.amqp._
import akka.stream.scaladsl.{Flow, Keep}
import akka.util.ByteString

import scala.concurrent.Future

object AmqpRpcFlow {

  /**
   * Scala API:
   * Create an [[https://www.rabbitmq.com/tutorials/tutorial-six-java.html RPC style flow]] for processing and communicating
   * over a rabbitmq message bus. This will create a private queue, and add the reply-to header to messages sent out.
   *
   * This stage materializes to a Future[String], which is the name of the private exclusive queue used for RPC communication.
   *
   * @param repliesPerMessage The number of responses that should be expected for each message placed on the queue. This
   *                            can be overridden per message by including `expectedReplies` in the the header of the [[WriteMessage]]
   */
  def simple(settings: AmqpWriteSettings, repliesPerMessage: Int = 1): Flow[ByteString, ByteString, Future[String]] =
    Flow[ByteString]
      .map(bytes => WriteMessage(bytes))
      .viaMat(atMostOnceFlow(settings, 1, repliesPerMessage))(Keep.right)
      .map(_.bytes)

  /**
   * Scala API:
   * Convenience for "at-most once delivery" semantics. Each message is acked to RabbitMQ
   * before it is emitted downstream.
   */
  def atMostOnceFlow(settings: AmqpWriteSettings,
                     bufferSize: Int,
                     repliesPerMessage: Int = 1): Flow[WriteMessage, ReadResult, Future[String]] =
    committableFlow(settings, bufferSize, repliesPerMessage)
      .mapAsync(1) { cm =>
        cm.ack().map(_ => cm.message)(ExecutionContexts.parasitic)
      }

  /**
   * Scala API:
   * The `committableFlow` makes it possible to commit (ack/nack) messages to RabbitMQ.
   * This is useful when "at-least once delivery" is desired, as each message will likely be
   * delivered one time but in failure cases could be duplicated.
   *
   * If you commit the offset before processing the message you get "at-most once delivery" semantics,
   * and for that there is a [[#atMostOnceFlow]].
   *
   * Compared to auto-commit, this gives exact control over when a message is considered consumed.
   */
  def committableFlow(settings: AmqpWriteSettings,
                      bufferSize: Int,
                      repliesPerMessage: Int = 1): Flow[WriteMessage, CommittableReadResult, Future[String]] =
    Flow.fromGraph(new impl.AmqpRpcFlowStage(settings, bufferSize, repliesPerMessage))

}
