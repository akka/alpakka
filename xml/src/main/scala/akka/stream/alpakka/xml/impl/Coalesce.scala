/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.xml.impl
import akka.annotation.InternalApi
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.alpakka.xml.{Characters, ParseEvent, TextEvent}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

/**
 * INTERNAL API
 */
@InternalApi private[xml] class Coalesce(maximumTextLength: Int) extends GraphStage[FlowShape[ParseEvent, ParseEvent]] {
  val in: Inlet[ParseEvent] = Inlet("XMLCoalesce.in")
  val out: Outlet[ParseEvent] = Outlet("XMLCoalesce.out")
  override val shape: FlowShape[ParseEvent, ParseEvent] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private var isBuffering = false
      private val buffer = new StringBuilder

      override def onPull(): Unit = pull(in)

      override def onPush(): Unit = grab(in) match {
        case t: TextEvent =>
          if (t.text.length + buffer.length > maximumTextLength)
            failStage(
              new IllegalStateException(
                s"Too long character sequence, maximum is $maximumTextLength but got " +
                s"${t.text.length + buffer.length - maximumTextLength} more "
              )
            )
          else {
            buffer.append(t.text)
            isBuffering = true
            pull(in)
          }
        case other =>
          if (isBuffering) {
            val coalesced = buffer.toString()
            isBuffering = false
            buffer.clear()
            emit(out, Characters(coalesced), () => emit(out, other, () => if (isClosed(in)) completeStage()))
          } else {
            push(out, other)
          }
      }

      override def onUpstreamFinish(): Unit =
        if (isBuffering) emit(out, Characters(buffer.toString()), () => completeStage())
        else completeStage()

      setHandlers(in, out, this)
    }
}
