/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.impl

import akka.Done
import akka.annotation.InternalApi
import akka.stream.alpakka.amqp.{AmqpPublishConfirmSettings, AmqpWriteSettings, WriteMessage}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{ActorAttributes, Attributes, Inlet, SinkShape}

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * Connects to an AMQP server upon materialization and sends write messages to the server.
 * Each materialized sink will create one connection to the broker.
 */
@InternalApi
private[amqp] final class AmqpSinkStage(settings: AmqpWriteSettings)
    extends GraphStageWithMaterializedValue[SinkShape[WriteMessage], Future[Done]] { stage =>

  val in = Inlet[WriteMessage]("AmqpSink.in")

  override def shape: SinkShape[WriteMessage] = SinkShape.of(in)

  override protected def initialAttributes: Attributes =
    super.initialAttributes and Attributes.name("AmqpSink") and ActorAttributes.IODispatcher

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    (new GraphStageLogic(shape) with AmqpConnectorLogic {
      override val settings = stage.settings
      private val exchange = settings.exchange.getOrElse("")
      private val routingKey = settings.routingKey.getOrElse("")

      override def whenConnected(): Unit = {
        if (settings.publishConfirm.isDefined) channel.confirmSelect()
        pull(in)
      }

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

          override def onPush(): Unit = {
            val elem = grab(in)
            channel.basicPublish(
              exchange,
              elem.routingKey.getOrElse(routingKey),
              elem.mandatory,
              elem.immediate,
              elem.properties.orNull,
              elem.bytes.toArray
            )

            settings.publishConfirm match {
              case Some(AmqpPublishConfirmSettings(confirmTimeout)) =>
                Try(channel.waitForConfirmsOrDie(confirmTimeout)) match {
                  case Success(_) => pull(in)
                  case Failure(e) => onFailure(e)
                }
              case None => pull(in)
            }

          }
        }
      )

      override def postStop(): Unit = {
        promise.tryFailure(new RuntimeException("stage stopped unexpectedly"))
        super.postStop()
      }

      override def onFailure(ex: Throwable): Unit = {
        promise.tryFailure(ex)
        super.onFailure(ex)
      }

    }, promise.future)
  }

  override def toString: String = "AmqpSink"
}
