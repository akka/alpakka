/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.client

import akka.Done
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{ActorAttributes, Attributes, Inlet, SinkShape}
import io.vertx.ext.stomp.{Frame, StompClientConnection}

import scala.concurrent.{Future, Promise}

/**
 * Connects to a STOMP server upon materialization and sends incoming messages to the server.
 * Each materialized sink will create one connection to the server.
 */
final class SinkStage(settings: ConnectorSettings)
    extends GraphStageWithMaterializedValue[SinkShape[SendingFrame], Future[Done]] {
  stage =>

  override def shape: SinkShape[SendingFrame] = SinkShape.of(in)

  override def toString: String = "StompClientSink"

  override protected def initialAttributes: Attributes = SinkStage.defaultAttributes

  val in = Inlet[SendingFrame]("StompClientSink.in")

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val thePromise = Promise[Done]()
    (new GraphStageLogic(shape) with ConnectorLogic {
      override val settings: ConnectorSettings = stage.settings
      override val promise: Promise[Done] = thePromise

      override def receiveHandler(connection: StompClientConnection): Unit = ()

      override def whenConnected(): Unit = pull(in)

      setHandler(
        in,
        new InHandler {

          override def onUpstreamFailure(ex: Throwable): Unit = {
            promise.tryFailure(ex)
            super.onUpstreamFailure(ex)
          }

          override def onUpstreamFinish(): Unit = {
            promise.trySuccess(Done)
            super.onUpstreamFinish()
          }

          override def onPush(): Unit = {
            val originalFrame: SendingFrame = grab(in)

            // mutable vertxFrame
            val vertxFrame = originalFrame.toVertexFrame
            // will not override if already set
            settings.destination.foreach(vertxFrame.setDestination)
            val continue = getAsyncCallback[Frame] { _ =>
              pull(in)
            }
            if (settings.withAck) {
              connection.send(vertxFrame, (frame: Frame) => {
                // server receive the message, continue pulling
                continue.invoke(frame)
              })
            } else {
              connection.send(vertxFrame)
              pull(in)
            }

          }
        }
      )

      override def postStop(): Unit = {
        promise.tryFailure(new RuntimeException("stage stopped unexpectedly"))
        super.postStop()
      }

      override def onFailure(ex: Throwable): Unit =
        promise.tryFailure(ex)

    }, thePromise.future)
  }

}

object SinkStage {
  private val defaultAttributes =
    Attributes.name("StompClientSink").and(ActorAttributes.dispatcher("akka.stream.default-blocking-io-dispatcher"))
}
