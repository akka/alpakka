/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.japi.Pair
import akka.stream.alpakka.amqp._
import akka.stream.javadsl.{Flow, Keep}
import akka.util.ByteString

import scala.compat.java8.FutureConverters._

object AmqpPublishFlow {

  /**
   * Java API: Creates an [[AmqpPublishFlow]] that accepts (ByteString, passthrough) elements.
   *
   * This stage materializes to a CompletionStage<Done>, which can be used to know when the Flow completes, either normally
   * or because of an amqp failure
   */
  def createSimple[O](settings: AmqpSinkSettings): Flow[Pair[ByteString, O], O, CompletionStage[Done]] =
    Flow
      .create[Pair[ByteString, O]]()
      .map(pair => pair.toScala)
      .viaMat[O, CompletionStage[Done], CompletionStage[Done]](
        scaladsl.AmqpPublishFlow
          .simple[O](settings)
          .mapMaterializedValue(f => f.toJava)
          .asJava
          .asInstanceOf[Flow[(ByteString, O), O, CompletionStage[Done]]],
        Keep.right
      )

  /**
   * Java API: Creates an [[AmqpPublishFlow]] that accepts ([[OutgoingMessage]], passthrough) elements.
   *
   * This stage materializes to a CompletionStage<Done>, which can be used to know when the Flow completes, either normally
   * or because of an amqp failure
   */
  def create[O](settings: AmqpSinkSettings): Flow[Pair[OutgoingMessage, O], O, CompletionStage[Done]] =
    Flow
      .create[Pair[OutgoingMessage, O]]()
      .map(pair => pair.toScala)
      .viaMat[O, CompletionStage[Done], CompletionStage[Done]](
        scaladsl
          .AmqpPublishFlow[O](settings)
          .mapMaterializedValue(f => f.toJava)
          .asJava
          .asInstanceOf[Flow[(OutgoingMessage, O), O, CompletionStage[Done]]],
        Keep.right
      )
}
