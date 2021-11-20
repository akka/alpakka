/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.csv.impl

import akka.annotation.InternalApi
import akka.event.Logging
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString

import scala.annotation.tailrec
import scala.util.control.NonFatal

/**
 * Internal API: Use [[akka.stream.alpakka.csv.scaladsl.CsvParsing]] instead.
 */
@InternalApi private[csv] class CsvParsingStage(delimiter: Byte,
                                                quoteChar: Byte,
                                                escapeChar: Byte,
                                                maximumLineLength: Int)
    extends GraphStage[FlowShape[ByteString, List[ByteString]]] {

  private val in = Inlet[ByteString](Logging.simpleName(this) + ".in")
  private val out = Outlet[List[ByteString]](Logging.simpleName(this) + ".out")
  override val shape = FlowShape(in, out)

  override protected def initialAttributes: Attributes = Attributes.name("CsvParsing")

  override def createLogic(inheritedAttributes: Attributes) =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private[this] val buffer = new CsvParser(delimiter, quoteChar, escapeChar, maximumLineLength)

      setHandlers(in, out, this)

      override def onPush(): Unit = {
        buffer.offer(grab(in))
        tryPollBuffer()
      }

      override def onPull(): Unit =
        tryPollBuffer()

      override def onUpstreamFinish(): Unit = {
        emitRemaining()
        completeStage()
      }

      private def tryPollBuffer() =
        try buffer.poll(requireLineEnd = true) match {
          case Some(csvLine) ⇒ push(out, csvLine)
          case _ ⇒
            if (isClosed(in)) {
              emitRemaining()
              completeStage()
            } else pull(in)
        } catch {
          case NonFatal(ex) ⇒ failStage(ex)
        }

      @tailrec private def emitRemaining(): Unit =
        buffer.poll(requireLineEnd = false) match {
          case Some(csvLine) ⇒
            emit(out, csvLine)
            emitRemaining()
          case _ ⇒
        }

    }
}
