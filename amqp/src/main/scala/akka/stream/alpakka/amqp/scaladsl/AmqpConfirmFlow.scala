/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.scaladsl

import akka.Done
import akka.stream.alpakka.amqp.{AmqpWriteSettings, ConfirmMessage, WriteMessage}
import akka.stream.alpakka.amqp.impl.AmqpConfirmFlowStage
import akka.stream.scaladsl.Flow

import scala.concurrent.Future

object AmqpConfirmFlow {

  /**
   * Scala API:
   *
   * Connects to an AMQP server upon materialization and sends incoming messages to the server.
   * The flow sends ConfirmMessage with ack or nack to the out port.
   *
   * This stage materializes to a Future[Done], which can be used to know when the Sink completes, either normally
   * or because of an amqp failure
   */
  def apply(settings: AmqpWriteSettings): Flow[WriteMessage, ConfirmMessage, Future[Done]] =
    Flow.fromGraph(new AmqpConfirmFlowStage(settings))

}
