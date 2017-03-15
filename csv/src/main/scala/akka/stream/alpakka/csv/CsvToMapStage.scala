/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.csv

import java.nio.charset.Charset

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString

import scala.collection.immutable

/**
 * Scala API: Converts incoming [[List[ByteString]]] to [[Map[String, ByteString]]].
 * @see akka.stream.alpakka.csv.CsvToMapJavaStage
 *
 * @param columnNames If given, these names are used as map keys; if not first stream element is used
 * @param charset Character set used to convert header line ByteString to String
 */
class CsvToMapStage(columnNames: Option[List[String]], charset: Charset)
    extends GraphStage[FlowShape[List[ByteString], Map[String, ByteString]]] {

  private val in = Inlet[List[ByteString]]("CsvToMap.in")
  private val out = Outlet[Map[String, ByteString]]("CsvToMap.out")
  override val shape = FlowShape.of(in, out)

  private var headers = columnNames

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val elem = grab(in)
            if (headers.isEmpty) {
              headers = Some(elem.map(_.decodeString(charset)))
              pull(in)
            } else {
              val map = headers.get.zip(elem).toMap
              emit(out, map)
            }
          }
        }
      )

      setHandler(out,
        new OutHandler {
        override def onPull(): Unit = pull(in)
      })
    }
}
