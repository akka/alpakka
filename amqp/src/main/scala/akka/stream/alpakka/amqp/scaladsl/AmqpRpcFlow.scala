/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.amqp.scaladsl

import akka.stream.alpakka.amqp.{AmqpRpcFlowStage, AmqpSinkSettings, IncomingMessage, OutgoingMessage}
import akka.stream.scaladsl.{Flow, Keep, Sink}
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
   *                            can be overridden per message by including `expectedReplies` in the the header of the [[OutgoingMessage]]
   */
  def simple(settings: AmqpSinkSettings, repliesPerMessage: Int = 1): Flow[ByteString, ByteString, Future[String]] =
    Flow[ByteString]
      .map(bytes => OutgoingMessage(bytes, false, false, None))
      .viaMat(apply(settings, 1, repliesPerMessage))(Keep.right)
      .map(_.bytes)

  /**
   * Scala API:
   * Create an [[https://www.rabbitmq.com/tutorials/tutorial-six-java.html RPC style flow]] for processing and communicating
   * over a rabbitmq message bus. This will create a private queue, and add the reply-to header to messages sent out.
   *
   * This stage materializes to a Future[String], which is the name of the private exclusive queue used for RPC communication.
   *
   * @param repliesPerMessage The number of responses that should be expected for each message placed on the queue. This
   *                            can be overridden per message by including `expectedReplies` in the the header of the [[OutgoingMessage]]
   */
  def apply(settings: AmqpSinkSettings,
            bufferSize: Int,
            repliesPerMessage: Int = 1): Flow[OutgoingMessage, IncomingMessage, Future[String]] =
    Flow.fromGraph(new AmqpRpcFlowStage(settings, bufferSize, repliesPerMessage))

}
