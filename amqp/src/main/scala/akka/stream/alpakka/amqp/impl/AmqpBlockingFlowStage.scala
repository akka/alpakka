/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.impl

import akka.Done
import akka.annotation.InternalApi
import akka.event.Logging
import akka.stream.{ActorAttributes, Attributes, FlowShape, Inlet, Outlet}
import akka.stream.alpakka.amqp.{AmqpWriteSettings, WriteMessage, WriteResult}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue}

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

/**
 * Internal API.
 *
 * AMQP flow that waits for confirmation after every published manner in a blocking way.
 * Messages are emitted as failed in case when `channel.waitForConfirms` either returns `false`, or throws
 * [[java.util.concurrent.TimeoutException]]. In case of any other exception during publication or waiting for
 * confirmation the stage is failed.
 */
@InternalApi private[amqp] final class AmqpBlockingFlowStage[T](
    settings: AmqpWriteSettings,
    confirmationTimeout: FiniteDuration
) extends GraphStageWithMaterializedValue[FlowShape[(WriteMessage, T), (WriteResult, T)], Future[Done]] { stage =>

  private val in: Inlet[(WriteMessage, T)] = Inlet(Logging.simpleName(this) + ".in")
  private val out: Outlet[(WriteResult, T)] = Outlet(Logging.simpleName(this) + ".out")

  override val shape: FlowShape[(WriteMessage, T), (WriteResult, T)] = FlowShape.of(in, out)

  override protected def initialAttributes: Attributes =
    Attributes.name(Logging.simpleName(this)) and ActorAttributes.IODispatcher

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val streamCompletion = Promise[Done]()
    (new AbstractAmqpFlowStageLogic[T](settings, streamCompletion, shape) {

      override def whenConnected(): Unit =
        channel.confirmSelect()

      override def publish(message: WriteMessage, passThrough: T): Unit = {
        val publicationResult: Try[Boolean] = for {
          _ <- publishWithBlockingConfirm(message)
          result <- waitForConfirmation()
        } yield result

        log.debug("Publication result: {}", publicationResult)

        publicationResult
          .map(result => push(out, (WriteResult(result), passThrough)))
          .recover { case t => onFailure(t) }
      }

      private def publishWithBlockingConfirm(message: WriteMessage): Try[Unit] = {
        log.debug("Publishing message {}.", message)
        Try(
          channel.basicPublish(
            settings.exchange.getOrElse(""),
            message.routingKey.orElse(settings.routingKey).getOrElse(""),
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

    }, streamCompletion.future)
  }
}
