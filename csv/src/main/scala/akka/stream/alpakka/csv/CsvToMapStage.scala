/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.csv

import java.nio.charset.Charset

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString

import scala.collection.immutable

/**
 * Internal API: Converts incoming [[List[ByteString]]] to [[Map[String, ByteString]]].
 * @see akka.stream.alpakka.csv.CsvToMapJavaStage
 *
 * @param columnNames If given, these names are used as map keys; if not first stream element is used
 * @param charset Character set used to convert header line ByteString to String
 */
private[csv] class CsvToMapStage(columnNames: Option[immutable.Seq[String]], charset: Charset)
    extends GraphStage[FlowShape[immutable.Seq[ByteString], Map[String, ByteString]]] {

  override protected def initialAttributes: Attributes = Attributes.name("CsvToMap")

  private val in = Inlet[immutable.Seq[ByteString]]("CsvToMap.in")
  private val out = Outlet[Map[String, ByteString]]("CsvToMap.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private var headers = columnNames

      setHandlers(in, out, this)

      override def onPush(): Unit = {
        val elem = grab(in)
        if (headers.isDefined) {
          val map = headers.get.zip(elem).toMap
          push(out, map)
        } else {
          headers = Some(elem.map(_.decodeString(charset)))
          pull(in)
        }
      }

      override def onPull(): Unit = pull(in)
    }
}
