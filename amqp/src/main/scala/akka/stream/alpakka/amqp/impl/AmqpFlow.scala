/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.impl

import akka.Done
import akka.annotation.InternalApi
import akka.event.Logging
import akka.stream._
import akka.stream.alpakka.amqp.{AmqpWriteSettings, WriteMessage, WriteResult}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler, StageLogging}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * Internal API.
 *
 * Base stage for AMQP flows that don't use asynchronous confirmations.
 */
@InternalApi private sealed abstract class AbstractAmqpFlowStage[T](
    override val settings: AmqpWriteSettings,
    promise: Promise[Done],
    shape: FlowShape[WriteMessage[T], WriteResult[T]]
) extends GraphStageLogic(shape)
    with AmqpConnectorLogic
    with StageLogging {

  private def in = shape.in
  private def out = shape.out

  override def whenConnected(): Unit = ()

  setHandler(
    in,
    new InHandler {
      override def onUpstreamFailure(ex: Throwable): Unit = {
        promise.failure(ex)
        super.onUpstreamFailure(ex)
      }

      override def onUpstreamFinish(): Unit = {
        promise.success(Done)
        super.onUpstreamFinish()
      }

      override def onPush(): Unit =
        publish(grab(in))
    }
  )

  protected def publish(message: WriteMessage[T]): Unit

  setHandler(out, new OutHandler {
    override def onPull(): Unit =
      if (!hasBeenPulled(in)) tryPull(in)
  })

  override def postStop(): Unit = {
    promise.tryFailure(new RuntimeException("Stage stopped unexpectedly."))
    super.postStop()
  }

  override def onFailure(ex: Throwable): Unit = {
    promise.tryFailure(ex)
    super.onFailure(ex)
  }
}

/**
 * Internal API.
 *
 * Simplest AMQP flow. Published messages without any delivery guarantees. Result is emitted as soon as message is
 * published. Because there it's not verified whether message is confirmed or rejected, results are emitted always
 * as confirmed. Since result is always confirmed, it would also make sense to emit just plain `passThrough` object
 * instead of complete [[WriteResult]] (possibly it would be less confusing for users), but [[WriteResult]] is used
 * for consistency with other variants and to make the flow ready for any possible future [[WriteResult]] extensions.
 */
@InternalApi private[amqp] final class AmqpSimpleFlow[T](writeSettings: AmqpWriteSettings)
    extends GraphStageWithMaterializedValue[FlowShape[WriteMessage[T], WriteResult[T]], Future[Done]] { stage =>

  private val in: Inlet[WriteMessage[T]] = Inlet(Logging.simpleName(this) + ".in")
  private val out: Outlet[WriteResult[T]] = Outlet(Logging.simpleName(this) + ".out")

  override val shape: FlowShape[WriteMessage[T], WriteResult[T]] = FlowShape.of(in, out)

  override protected def initialAttributes: Attributes =
    Attributes.name(Logging.simpleName(this)) and ActorAttributes.IODispatcher

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    (new AbstractAmqpFlowStage[T](writeSettings, promise, shape) {
      override def publish(message: WriteMessage[T]): Unit = {
        log.debug("Publishing message {}.", message)

        channel.basicPublish(
          writeSettings.exchange.getOrElse(""),
          message.routingKey.orElse(writeSettings.routingKey).getOrElse(""),
          message.mandatory,
          message.immediate,
          message.properties.orNull,
          message.bytes.toArray
        )
        push(out, WriteResult.confirmed(message.passThrough))
      }
    }, promise.future)
  }
}

/**
 * Internal API.
 *
 * AMQP flow that waits for confirmation after every published manner in a blocking way.
 * Messages are emitted as failed in case when `channel.waitForConfirms` either returns `false`, or throws
 * [[java.util.concurrent.TimeoutException]]. In case of any other exception during publication or waiting for
 * confirmation the stage is failed.
 */
@InternalApi private[amqp] final class AmqpBlockingFlow[T](
    writeSettings: AmqpWriteSettings,
    confirmationTimeout: FiniteDuration
) extends GraphStageWithMaterializedValue[FlowShape[WriteMessage[T], WriteResult[T]], Future[Done]] { stage =>

  private val in: Inlet[WriteMessage[T]] = Inlet(Logging.simpleName(this) + ".in")
  private val out: Outlet[WriteResult[T]] = Outlet(Logging.simpleName(this) + ".out")

  override val shape: FlowShape[WriteMessage[T], WriteResult[T]] = FlowShape.of(in, out)

  override protected def initialAttributes: Attributes =
    Attributes.name(Logging.simpleName(this)) and ActorAttributes.IODispatcher

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    (new AbstractAmqpFlowStage[T](writeSettings, promise, shape) {

      override def whenConnected(): Unit =
        channel.confirmSelect()

      override def publish(message: WriteMessage[T]): Unit = {
        val publicationResult: Try[Boolean] = for {
          _ <- publishWithBlockingConfirm(message)
          result <- waitForConfirmation()
        } yield result

        log.debug("Publication result: {}", publicationResult)

        publicationResult
          .map(result => push(out, WriteResult(result, message.passThrough)))
          .recover { case t => onFailure(t) }
      }

      private def publishWithBlockingConfirm(message: WriteMessage[T]): Try[Unit] = {
        log.debug("Publishing message {}.", message)
        Try(
          channel.basicPublish(
            writeSettings.exchange.getOrElse(""),
            message.routingKey.orElse(writeSettings.routingKey).getOrElse(""),
            message.mandatory,
            message.immediate,
            message.properties.orNull,
            message.bytes.toArray
          )
        )
      }

      private def waitForConfirmation(): Try[Boolean] = {
        log.debug("Waiting for message confirmation.")
        Try(channel.waitForConfirms(confirmationTimeout.toMillis))
          .recoverWith {
            case _: java.util.concurrent.TimeoutException => Success(false)
            case throwable => Failure(throwable)
          }

      }

    }, promise.future)
  }
}
