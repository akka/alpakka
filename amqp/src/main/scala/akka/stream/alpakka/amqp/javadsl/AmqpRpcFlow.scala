/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.amqp.javadsl

import java.util.concurrent.CompletionStage

import akka.stream.alpakka.amqp._
import akka.stream.javadsl.Flow
import akka.util.ByteString

import scala.compat.java8.FutureConverters._

object AmqpRpcFlow {

  /**
   * Java API:
   * Create an [[https://www.rabbitmq.com/tutorials/tutorial-six-java.html RPC style flow]] for processing and communicating
   * over a rabbitmq message bus. This will create a private queue, and add the reply-to header to messages sent out.
   *
   * This stage materializes to a CompletionStage<String>, which is the name of the private exclusive queue used for RPC communication.
   */
  def create(settings: AmqpSinkSettings,
             bufferSize: Int): Flow[OutgoingMessage, IncomingMessage, CompletionStage[String]] =
    akka.stream.alpakka.amqp.scaladsl.AmqpRpcFlow(settings, bufferSize).mapMaterializedValue(f => f.toJava).asJava

  /**
   * Java API:
   * Create an [[https://www.rabbitmq.com/tutorials/tutorial-six-java.html RPC style flow]] for processing and communicating
   * over a rabbitmq message bus. This will create a private queue, and add the reply-to header to messages sent out.
   *
   * This stage materializes to a CompletionStage<String>, which is the name of the private exclusive queue used for RPC communication.
   *
   * @param repliesPerMessage The number of responses that should be expected for each message placed on the queue. This
   *                            can be overridden per message by including `expectedReplies` in the the header of the [[OutgoingMessage]]
   */
  def create(settings: AmqpSinkSettings,
             bufferSize: Int,
             repliesPerMessage: Int): Flow[OutgoingMessage, IncomingMessage, CompletionStage[String]] =
    akka.stream.alpakka.amqp.scaladsl
      .AmqpRpcFlow(settings, bufferSize, repliesPerMessage)
      .mapMaterializedValue(f => f.toJava)
      .asJava

  /**
   * Java API:
   * Create an [[https://www.rabbitmq.com/tutorials/tutorial-six-java.html RPC style flow]] for processing and communicating
   * over a rabbitmq message bus. This will create a private queue, and add the reply-to header to messages sent out.
   *
   * This stage materializes to a CompletionStage<String>, which is the name of the private exclusive queue used for RPC communication.
   *
   * @param repliesPerMessage The number of responses that should be expected for each message placed on the queue. This
   *                            can be overridden per message by including `expectedReplies` in the the header of the [[OutgoingMessage]]
   */
  def createSimple(settings: AmqpSinkSettings,
                   repliesPerMessage: Int): Flow[ByteString, ByteString, CompletionStage[String]] =
    akka.stream.alpakka.amqp.scaladsl.AmqpRpcFlow
      .simple(settings, repliesPerMessage)
      .mapMaterializedValue(f => f.toJava)
      .asJava

}
