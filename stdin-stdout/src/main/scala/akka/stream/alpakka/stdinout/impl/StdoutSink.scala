/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stdinout.impl

import akka.annotation.InternalApi
import akka.stream.{Attributes, Inlet, SinkShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, StageLogging}

/**
 * INTERNAL API
 */
@InternalApi
private[stdinout] final class StdoutSink extends GraphStage[SinkShape[String]] {

  val in: Inlet[String] = Inlet("StdoutSink")
  override val shape: SinkShape[String] = SinkShape(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new StdoutSinkStageLogic(shape)
}

/**
 * INTERNAL API
 */
@InternalApi
private[stdinout] final class StdoutSinkStageLogic(shape: SinkShape[String])
    extends GraphStageLogic(shape)
    with StageLogging {

  private def in: Inlet[String] = shape.in

  private def pullWithLogging(in: Inlet[String]): Unit = {
    pull(in)
    log.debug("StdoutSink has made an upstream pull")
  }

  // This requests one element at the Sink startup.
  override def preStart(): Unit = pullWithLogging(in)

  setHandler(
    in,
    new InHandler {
      override def onPush(): Unit = {
        val incomingString: String = grab(in)
        log.debug(s"StdoutSink has received a push containing: $incomingString")

        println(incomingString)
        pullWithLogging(in)
      }
    }
  )

}
