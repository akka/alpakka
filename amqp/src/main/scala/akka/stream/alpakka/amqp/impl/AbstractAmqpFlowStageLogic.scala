/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.amqp.impl

import akka.Done
import akka.annotation.InternalApi
import akka.stream._
import akka.stream.alpakka.amqp.{AmqpWriteSettings, WriteMessage, WriteResult}
import akka.stream.stage.{GraphStageLogic, InHandler, OutHandler, StageLogging}

import scala.concurrent.Promise

/**
 * Internal API.
 *
 * Base stage for AMQP flows that don't use asynchronous confirmations.
 */
@InternalApi private abstract class AbstractAmqpFlowStageLogic[T](
    override val settings: AmqpWriteSettings,
    streamCompletion: Promise[Done],
    shape: FlowShape[(WriteMessage, T), (WriteResult, T)]
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
        streamCompletion.failure(ex)
        super.onUpstreamFailure(ex)
      }

      override def onUpstreamFinish(): Unit = {
        streamCompletion.success(Done)
        super.onUpstreamFinish()
      }

      override def onPush(): Unit = {
        val (message, passThrough) = grab(in)
        publish(message, passThrough)
      }
    }
  )

  protected def publish(message: WriteMessage, passThrough: T): Unit

  setHandler(out, new OutHandler {
    override def onPull(): Unit =
      if (!hasBeenPulled(in)) tryPull(in)
  })

  override def postStop(): Unit = {
    streamCompletion.tryFailure(new RuntimeException("Stage stopped unexpectedly."))
    super.postStop()
  }

  override def onFailure(ex: Throwable): Unit = {
    streamCompletion.tryFailure(ex)
    super.onFailure(ex)
  }
}
