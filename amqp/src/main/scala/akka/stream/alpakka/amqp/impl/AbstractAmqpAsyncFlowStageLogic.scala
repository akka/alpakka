/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.impl

import akka.Done
import akka.annotation.InternalApi
import akka.stream._
import akka.stream.alpakka.amqp.impl.AbstractAmqpAsyncFlowStageLogic.DeliveryTag
import akka.stream.alpakka.amqp.{AmqpWriteSettings, WriteMessage, WriteResult}
import akka.stream.stage._
import com.rabbitmq.client.ConfirmCallback

import scala.collection.mutable
import scala.concurrent.Promise

/**
 * Internal API.
 */
@InternalApi private final case class AwaitingMessage[T](
    tag: DeliveryTag,
    passThrough: T,
    ready: Boolean = false
)

/**
 * Internal API.
 */
@InternalApi private object AbstractAmqpAsyncFlowStageLogic {
  type DeliveryTag = Long
}

/**
 * Internal API.
 *
 * Base stage for AMQP flows with asynchronous confirmations.
 */
@InternalApi private abstract class AbstractAmqpAsyncFlowStageLogic[T](
    override val settings: AmqpWriteSettings,
    streamCompletion: Promise[Done],
    shape: FlowShape[(WriteMessage, T), (WriteResult, T)]
) extends TimerGraphStageLogic(shape)
    with AmqpConnectorLogic
    with StageLogging {

  import AbstractAmqpAsyncFlowStageLogic._

  private def in = shape.in
  private def out = shape.out

  private val exchange = settings.exchange.getOrElse("")
  private val routingKey = settings.routingKey.getOrElse("")

  private val exitQueue = mutable.Queue.empty[(WriteResult, T)]
  private var upstreamException: Option[Throwable] = None

  override def whenConnected(): Unit = {
    channel.confirmSelect()
    channel.addConfirmListener(asAsyncCallback(onConfirmation), asAsyncCallback(onRejection))
  }

  private def asAsyncCallback(confirmCallback: (DeliveryTag, Boolean) => Unit): ConfirmCallback = {
    val callback = getAsyncCallback[(DeliveryTag, Boolean)] {
      case (tag: DeliveryTag, multiple: Boolean) => confirmCallback(tag, multiple)
    }
    new ConfirmCallback { // cant use function literal because it doesn't work with 2.11
      override def handle(tag: DeliveryTag, multiple: Boolean): Unit = callback.invoke((tag, multiple))
    }
  }

  private def onConfirmation(tag: DeliveryTag, multiple: Boolean): Unit = {
    log.debug("Received confirmation for deliveryTag {} (multiple={}).", tag, multiple)

    val dequeued: Iterable[AwaitingMessage[T]] = dequeueAwaitingMessages(tag, multiple)

    dequeued.foreach(m => cancelTimer(m.tag))

    pushOrEnqueueResults(
      dequeued.map(m => (WriteResult.confirmed, m.passThrough))
    )
  }

  private def onRejection(tag: DeliveryTag, multiple: Boolean): Unit = {
    log.debug("Received rejection for deliveryTag {} (multiple={}).", tag, multiple)

    val dequeued: Iterable[AwaitingMessage[T]] = dequeueAwaitingMessages(tag, multiple)

    dequeued.foreach(m => cancelTimer(m.tag))

    pushOrEnqueueResults(
      dequeued.map(m => (WriteResult.rejected, m.passThrough))
    )
  }

  private def pushOrEnqueueResults(results: Iterable[(WriteResult, T)]): Unit = {
    results.foreach(
      result =>
        if (isAvailable(out) && exitQueue.isEmpty) {
          log.debug("Pushing {} downstream.", result)
          push(out, result)
        } else {
          log.debug("Message {} queued for downstream push.", result)
          exitQueue.enqueue(result)
        }
    )
    if (isFinished) closeStage()
  }

  override def postStop(): Unit = {
    streamCompletion.tryFailure(new RuntimeException("Stage stopped unexpectedly."))
    super.postStop()
  }

  override def onFailure(ex: Throwable): Unit = {
    streamCompletion.tryFailure(ex)
    onFailure(ex)
  }

  def dequeueAwaitingMessages(tag: DeliveryTag, multiple: Boolean): Iterable[AwaitingMessage[T]]

  def enqueueMessage(tag: DeliveryTag, passThrough: T): Unit

  def messagesAwaitingDelivery: Int

  def noAwaitingMessages: Boolean

  setHandler(
    in,
    new InHandler {

      override def onPush(): Unit = {
        val (message, passThrough) = grab(in)
        val tag = publish(message)

        scheduleOnce(tag, settings.confirmationTimeout)
        enqueueMessage(tag, passThrough)
        if (messagesAwaitingDelivery + exitQueue.size < settings.bufferSize && !hasBeenPulled(in)) tryPull(in)
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        upstreamException = Some(ex)
        if (isFinished)
          closeStage()
        else
          log.debug("Received upstream failure signal - stage will be failed when all buffered messages are processed")

      }

      override def onUpstreamFinish(): Unit =
        if (noAwaitingMessages && exitQueue.isEmpty) {
          streamCompletion.success(Done)
          super.onUpstreamFinish()
        } else {
          log.debug("Received upstream finish signal - stage will be closed when all buffered messages are processed")
        }

      private def publish(message: WriteMessage): DeliveryTag = {
        val tag: DeliveryTag = channel.getNextPublishSeqNo

        log.debug("Publishing message {} with deliveryTag {}.", message, tag)

        channel.basicPublish(
          exchange,
          message.routingKey.getOrElse(routingKey),
          message.mandatory,
          message.immediate,
          message.properties.orNull,
          message.bytes.toArray
        )

        tag
      }
    }
  )

  setHandler(
    out,
    new OutHandler {
      override def onPull(): Unit = {
        if (exitQueue.nonEmpty) {
          val result = exitQueue.dequeue()
          log.debug("Pushing enqueued {} downstream.", result)
          push(out, result)
        }

        if (isFinished) closeStage()
        else if (!hasBeenPulled(in)) tryPull(in)
      }
    }
  )

  override protected def onTimer(timerKey: Any): Unit =
    timerKey match {
      case tag: DeliveryTag => {
        log.debug("Received timeout for deliveryTag {}.", tag)
        onRejection(tag, multiple = false)
      }
      case _ => ()
    }

  private def closeStage(): Unit =
    upstreamException match {
      case Some(throwable) =>
        streamCompletion.failure(throwable)
        failStage(throwable)
      case None =>
        streamCompletion.success(Done)
        completeStage()
    }

  private def isFinished: Boolean = isClosed(in) && noAwaitingMessages && exitQueue.isEmpty
}
