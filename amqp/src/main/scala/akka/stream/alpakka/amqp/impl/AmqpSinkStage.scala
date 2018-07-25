/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.impl

import akka.Done
import akka.annotation.InternalApi
import akka.stream.alpakka.amqp.{AmqpSinkSettings, OutgoingMessage}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{ActorAttributes, Attributes, Inlet, SinkShape}

import scala.concurrent.{Future, Promise}

/**
 * Connects to an AMQP server upon materialization and sends incoming messages to the server.
 * Each materialized sink will create one connection to the broker.
 */
@InternalApi
private[amqp] final class AmqpSinkStage(settings: AmqpSinkSettings)
    extends GraphStageWithMaterializedValue[SinkShape[OutgoingMessage], Future[Done]] { stage =>

  val in = Inlet[OutgoingMessage]("AmqpSink.in")

  override def shape: SinkShape[OutgoingMessage] = SinkShape.of(in)

  override protected def initialAttributes: Attributes =
    Attributes
      .name("AmqpSink")
      .and(ActorAttributes.dispatcher("akka.stream.default-blocking-io-dispatcher"))

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    (new GraphStageLogic(shape) with AmqpConnectorLogic {
      override val settings = stage.settings
      private val exchange = settings.exchange.getOrElse("")
      private val routingKey = settings.routingKey.getOrElse("")

      override def whenConnected(): Unit = pull(in)

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
            pull(in)
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
