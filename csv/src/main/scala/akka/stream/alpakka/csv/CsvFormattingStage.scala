/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.csv

import akka.event.Logging
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString

import scala.collection.immutable

/**
 * Internal API: Use [[akka.stream.alpakka.csv.scaladsl.CsvFormatting]] instead.
 */
private[csv] class CsvFormattingStage(delimiter: Char,
                                      quoteChar: Char,
                                      escapeChar: Char,
                                      endOfLine: String,
                                      quotingStyle: CsvQuotingStyle,
                                      charsetName: String = ByteString.UTF_8)
    extends GraphStage[FlowShape[immutable.Seq[Any], ByteString]] {

  private val in = Inlet[immutable.Seq[Any]](Logging.simpleName(this) + ".in")
  private val out = Outlet[ByteString](Logging.simpleName(this) + ".out")
  override val shape = FlowShape(in, out)

  override protected def initialAttributes: Attributes = Attributes.name("CsvFormatting")

  override def createLogic(attributes: Attributes) =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private[this] val formatter =
        new CsvFormatter(delimiter, quoteChar, escapeChar, endOfLine, quotingStyle, charsetName)

      setHandlers(in, out, this)

      override def onPush(): Unit =
        push(out, formatter.toCsv(grab(in)))

      override def onPull(): Unit = pull(in)
    }
}
