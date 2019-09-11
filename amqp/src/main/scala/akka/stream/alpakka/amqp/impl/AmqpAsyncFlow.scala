/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.impl

import akka.Done
import akka.annotation.InternalApi
import akka.event.Logging
import akka.stream._
import akka.stream.alpakka.amqp.impl.AmqpAsyncFlowStageLogic.DeliveryTag
import akka.stream.alpakka.amqp.{AmqpWriteSettings, WriteMessage, WriteResult}
import akka.stream.stage._
import com.rabbitmq.client.ConfirmCallback

import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Promise}

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
@InternalApi private object AmqpAsyncFlowStageLogic {
  type DeliveryTag = Long
}

/**
 * Internal API.
 *
 * Base stage for AMQP flows with asynchronous confirmations.
 */
@InternalApi private sealed abstract class AmqpAsyncFlowStageLogic[T](
    override val settings: AmqpWriteSettings,
    bufferSize: Int,
    confirmationTimeout: FiniteDuration,
    promise: Promise[Done],
    shape: FlowShape[WriteMessage[T], WriteResult[T]]
) extends TimerGraphStageLogic(shape)
    with AmqpConnectorLogic
    with StageLogging {

  private def in = shape.in
  private def out = shape.out

  private val exchange = settings.exchange.getOrElse("")
  private val routingKey = settings.routingKey.getOrElse("")
  private val exitQueue = mutable.Queue.empty[WriteResult[T]]
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
      dequeued.map(m => WriteResult.confirmed(m.passThrough))
    )
  }

  private def onRejection(tag: DeliveryTag, multiple: Boolean): Unit = {
    log.debug("Received rejection for deliveryTag {} (multiple={}).", tag, multiple)

    val dequeued: Iterable[AwaitingMessage[T]] = dequeueAwaitingMessages(tag, multiple)

    dequeued.foreach(m => cancelTimer(m.tag))

    pushOrEnqueueResults(
      dequeued.map(m => WriteResult.rejected(m.passThrough))
    )
  }

  private def pushOrEnqueueResults(results: Iterable[WriteResult[T]]): Unit = {
    results.foreach(result =>
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
    promise.tryFailure(new RuntimeException("Stage stopped unexpectedly."))
    super.postStop()
  }

  override def onFailure(ex: Throwable): Unit = {
    promise.tryFailure(ex)
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
        val message: WriteMessage[T] = grab(in)
        val tag = publish(message)

        scheduleOnce(tag, confirmationTimeout)
        enqueueMessage(tag, message.passThrough)
        if (messagesAwaitingDelivery + exitQueue.size < bufferSize && !hasBeenPulled(in)) tryPull(in)
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
          promise.success(Done)
          super.onUpstreamFinish()
        } else {
          log.debug("Received upstream finish signal - stage will be closed when all buffered messages are processed")
        }

      private def publish(message: WriteMessage[T]): DeliveryTag = {
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
        promise.failure(throwable)
        failStage(throwable)
      case None =>
        promise.success(Done)
        completeStage()
    }

  private def isFinished: Boolean = isClosed(in) && noAwaitingMessages && exitQueue.isEmpty
}

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
@InternalApi private[amqp] final class AmqpAsyncFlow[T](
    sinkSettings: AmqpWriteSettings,
    bufferSize: Int,
    confirmationTimeout: FiniteDuration
) extends GraphStageWithMaterializedValue[FlowShape[WriteMessage[T], WriteResult[T]], Future[Done]] {

  val in: Inlet[WriteMessage[T]] = Inlet(Logging.simpleName(this) + ".in")
  val out: Outlet[WriteResult[T]] = Outlet(Logging.simpleName(this) + ".out")

  override def shape: FlowShape[WriteMessage[T], WriteResult[T]] = FlowShape.of(in, out)

  override protected def initialAttributes: Attributes =
    super.initialAttributes and Attributes.name(Logging.simpleName(this)) and ActorAttributes.IODispatcher

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    (new AmqpAsyncFlowStageLogic(sinkSettings, bufferSize, confirmationTimeout, promise, shape) {

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

    }, promise.future)
  }
}

/**
 * Internal API.
 *
 * AMQP flow that uses asynchronous confirmations in possibly the most efficient way.
 * Messages are dequeued and pushed downstream as soon as confirmation is received. Flag `ready` on [[AwaitingMessage]]
 * is not used in this case. Flag `multiple` on a confirmation means that broker confirms all messages up to a
 * given delivery tag, which means that so all messages up to (and including) this delivery tag can be safely dequeued.
 */
@InternalApi private[amqp] final class AmqpAsyncUnorderedFlow[T](
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
    val promise = Promise[Done]()
    (new AmqpAsyncFlowStageLogic(sinkSettings, bufferSize, confirmationTimeout, promise, shape) {

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

    }, promise.future)
  }
}
