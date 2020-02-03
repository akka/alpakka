/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.impl

import akka.Done
import akka.annotation.InternalApi
import akka.event.Logging
import akka.stream.alpakka.amqp.impl.AbstractAmqpAsyncFlowStageLogic.DeliveryTag
import akka.stream.alpakka.amqp.{AmqpWriteSettings, WriteMessage, WriteResult}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue}
import akka.stream._

import scala.collection.immutable.TreeMap
import scala.concurrent.{Future, Promise}

/**
 * Internal API.
 *
 * AMQP flow that uses asynchronous confirmations but preserves the order in which messages were pulled.
 * Internally messages awaiting confirmation are stored in a ordered buffer. Initially messages have `ready`
 * flag set to `false`. On confirmation flag is changed to `true` and messages from the beginning of the queue
 * are dequeued in sequence until first non-ready message - this means that if confirmed message is not a
 * first element of the buffer then nothing will be dequeued. Flag `multiple` on a confirmation means that
 * broker confirms all messages up to a given delivery tag, which means that so all messages up to (and including)
 * this delivery tag can be safely dequeued.
 */
@InternalApi private[amqp] final class AmqpAsyncFlowStage[T](
    settings: AmqpWriteSettings
) extends GraphStageWithMaterializedValue[FlowShape[(WriteMessage, T), (WriteResult, T)], Future[Done]] {

  val in: Inlet[(WriteMessage, T)] = Inlet(Logging.simpleName(this) + ".in")
  val out: Outlet[(WriteResult, T)] = Outlet(Logging.simpleName(this) + ".out")

  override def shape: FlowShape[(WriteMessage, T), (WriteResult, T)] = FlowShape.of(in, out)

  override protected def initialAttributes: Attributes =
    super.initialAttributes and Attributes.name(Logging.simpleName(this)) and ActorAttributes.IODispatcher

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val streamCompletion = Promise[Done]()
    (new AbstractAmqpAsyncFlowStageLogic(settings, streamCompletion, shape) {

      private var buffer = TreeMap[DeliveryTag, AwaitingMessage[T]]()

      override def enqueueMessage(tag: DeliveryTag, passThrough: T): Unit =
        buffer += (tag -> AwaitingMessage(tag, passThrough))

      override def dequeueAwaitingMessages(tag: DeliveryTag, multiple: Boolean): Iterable[AwaitingMessage[T]] =
        if (multiple) {
          dequeueWhile((t, _) => t <= tag)
        } else {
          setReady(tag)
          if (isAtHead(tag)) {
            dequeueWhile((_, message) => message.ready)
          } else {
            Seq.empty
          }
        }

      private def dequeueWhile(
          predicate: (DeliveryTag, AwaitingMessage[T]) => Boolean
      ): Iterable[AwaitingMessage[T]] = {
        val dequeued = buffer.takeWhile { case (k, v) => predicate(k, v) }
        buffer --= dequeued.keys
        dequeued.values
      }

      private def isAtHead(tag: DeliveryTag): Boolean =
        buffer.headOption.exists { case (tag, _) => tag == tag }

      private def setReady(tag: DeliveryTag): Unit =
        buffer.get(tag).foreach(message => buffer += (tag -> message.copy(ready = true)))

      override def messagesAwaitingDelivery: Int = buffer.size

      override def noAwaitingMessages: Boolean = buffer.isEmpty

    }, streamCompletion.future)
  }
}
