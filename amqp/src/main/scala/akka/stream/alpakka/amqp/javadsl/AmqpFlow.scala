/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.javadsl

import akka.NotUsed
import akka.japi.Pair
import akka.japi.function.Function
import akka.stream.alpakka.amqp._
import akka.stream.javadsl.{Flow, Keep}
import akka.util.ByteString

object AmqpFlow {

  /**
   * Java API: Creates an [[AmqpFlow]] that accepts (ByteString, passthrough) elements.
   *
   * This stage materializes to a CompletionStage<Done>, which can be used to know when the Flow completes, either normally
   * or because of an amqp failure
   */
  def createSimple[O](settings: AmqpWriteSettings): Flow[Pair[ByteString, O], O, NotUsed] =
    Flow
      .fromFunction(new Function[Pair[ByteString, O], (ByteString, O)] {
        override def apply(pair: Pair[ByteString, O]): (ByteString, O) = pair.toScala
      })
      .viaMat[O, NotUsed, NotUsed](
        scaladsl.AmqpFlow
          .simple[O](settings)
          .asJava
          .asInstanceOf[Flow[(ByteString, O), O, NotUsed]],
        Keep.right
      )

  /**
   * Java API: Creates an [[AmqpFlow]] that accepts ([[WriteMessage]], passthrough) elements.
   *
   * This stage materializes to a CompletionStage<Done>, which can be used to know when the Flow completes, either normally
   * or because of an amqp failure
   */
  def create[O](settings: AmqpWriteSettings): Flow[Pair[WriteMessage, O], O, NotUsed] =
    Flow
      .fromFunction(new Function[Pair[WriteMessage, O], (WriteMessage, O)] {
        override def apply(pair: Pair[WriteMessage, O]): (WriteMessage, O) = pair.toScala
      })
      .viaMat[O, NotUsed, NotUsed](
        scaladsl
          .AmqpFlow[O](settings)
          .asJava
          .asInstanceOf[Flow[(WriteMessage, O), O, NotUsed]],
        Keep.right
      )
}
