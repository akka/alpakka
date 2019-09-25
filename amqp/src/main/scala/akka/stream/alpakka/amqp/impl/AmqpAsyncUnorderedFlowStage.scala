/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.impl

import akka.Done
import akka.annotation.InternalApi
import akka.event.Logging
import akka.stream.{ActorAttributes, Attributes, FlowShape, Inlet, Outlet}
import akka.stream.alpakka.amqp.{AmqpWriteSettings, WriteMessage, WriteResult}
import akka.stream.alpakka.amqp.impl.AbstractAmqpAsyncFlowStageLogic.DeliveryTag
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue}

import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.FiniteDuration

/**
 * Internal API.
 *
 * AMQP flow that uses asynchronous confirmations in possibly the most efficient way.
 * Messages are dequeued and pushed downstream as soon as confirmation is received. Flag `ready` on [[AwaitingMessage]]
 * is not used in this case. Flag `multiple` on a confirmation means that broker confirms all messages up to a
 * given delivery tag, which means that so all messages up to (and including) this delivery tag can be safely dequeued.
 */
@InternalApi private[amqp] final class AmqpAsyncUnorderedFlowStage[T](
    sinkSettings: AmqpWriteSettings,
    bufferSize: Int,
    confirmationTimeout: FiniteDuration
) extends GraphStageWithMaterializedValue[FlowShape[WriteMessage[T], WriteResult[T]], Future[Done]] {

  private val in: Inlet[WriteMessage[T]] = Inlet(Logging.simpleName(this) + ".in")
  private val out: Outlet[WriteResult[T]] = Outlet(Logging.simpleName(this) + ".out")

  override val shape: FlowShape[WriteMessage[T], WriteResult[T]] = FlowShape.of(in, out)

  override protected def initialAttributes: Attributes =
    super.initialAttributes and Attributes.name(Logging.simpleName(this)) and ActorAttributes.IODispatcher

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val streamCompletion = Promise[Done]()
    (new AbstractAmqpAsyncFlowStageLogic(sinkSettings, bufferSize, confirmationTimeout, streamCompletion, shape) {

      private val buffer = mutable.Queue.empty[AwaitingMessage[T]]

      override def enqueueMessage(tag: DeliveryTag, passThrough: T): Unit =
        buffer += AwaitingMessage(tag, passThrough)

      override def dequeueAwaitingMessages(tag: DeliveryTag, multiple: Boolean): Iterable[AwaitingMessage[T]] =
        if (multiple)
          buffer.dequeueAll(_.tag <= tag)
        else
          buffer
            .dequeueFirst(_.tag == tag)
            .fold(Seq.empty[AwaitingMessage[T]])(Seq(_))

      override def messagesAwaitingDelivery: Int = buffer.length

      override def noAwaitingMessages: Boolean = buffer.isEmpty

    }, streamCompletion.future)
  }
}
