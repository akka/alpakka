/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.csv.scaladsl

import akka.NotUsed
import akka.event.Logging
import akka.stream.alpakka.csv.CsvParser
import akka.stream.scaladsl.Flow
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString

import scala.util.control.NonFatal

/** Provides CSV framing stages that can separate CSV lines from incoming [[ByteString]] objects. */
object CsvFraming {

  val backslash: Byte = '\\'
  val comma: Byte = ','
  val doubleQuote: Byte = '"'

  def lineScanner(delimiter: Byte = comma,
                  quoteChar: Byte = doubleQuote,
                  escapeChar: Byte = backslash): Flow[ByteString, List[ByteString], NotUsed] =
    Flow[ByteString].via(new GraphStage[FlowShape[ByteString, List[ByteString]]] {

      private val in = Inlet[ByteString](Logging.simpleName(this) + ".in")
      private val out = Outlet[List[ByteString]](Logging.simpleName(this) + ".out")
      override val shape = FlowShape(in, out)

      override protected def initialAttributes: Attributes = Attributes.name("CsvFraming.lineScanner")

      override def createLogic(inheritedAttributes: Attributes) =
        new GraphStageLogic(shape) with InHandler with OutHandler {
          private val buffer = new CsvParser(delimiter, quoteChar, escapeChar)

          setHandlers(in, out, this)

          override def onPush(): Unit = {
            buffer.offer(grab(in))
            tryPollBuffer()
          }

          override def onPull(): Unit =
            tryPollBuffer()

          override def onUpstreamFinish(): Unit =
            buffer.poll() match {
              case Some(csvLine) ⇒ emit(out, csvLine)
              case _ ⇒ completeStage()
            }

          private def tryPollBuffer() =
            try buffer.poll() match {
              case Some(csvLine) ⇒ push(out, csvLine)
              case _ ⇒ if (isClosed(in)) completeStage() else pull(in)
            } catch {
              case NonFatal(ex) ⇒ failStage(ex)
            }
        }
    })

}
