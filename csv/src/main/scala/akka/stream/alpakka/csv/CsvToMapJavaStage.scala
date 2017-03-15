/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.csv

import java.nio.charset.Charset
import java.util.function.Function
import java.util.stream.Collectors
import java.{util => ju}

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString

/**
 * Java API: Converts incoming {@link Collection}<{@link ByteString}> to {@link java.util.Map}<String, ByteString>.
 *
 * @param columnNames If given, these names are used as map keys; if not first stream element is used
 * @param charset Character set used to convert header line ByteString to String
 */
class CsvToMapJavaStage(columnNames: Option[ju.Collection[String]], charset: Charset)
    extends GraphStage[FlowShape[ju.Collection[ByteString], ju.Map[String, ByteString]]] {

  private val in = Inlet[ju.Collection[ByteString]]("CsvToMap.in")
  private val out = Outlet[ju.Map[String, ByteString]]("CsvToMap.out")
  override val shape = FlowShape.of(in, out)

  private var headers = columnNames
  private val decodeByteString = new java.util.function.Function[ByteString, String]() {
    override def apply(t: ByteString): String = t.decodeString(charset)
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val elem = grab(in)
            if (headers.isEmpty) {
              headers = Some(decode(elem))
              pull(in)
            } else {
              val map = zipWithHeaders(elem)
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

  private def decode(elem: ju.Collection[ByteString]) = {
    elem.stream().map[String](decodeByteString).collect(Collectors.toList())
  }

  private def zipWithHeaders(elem: ju.Collection[ByteString]): ju.Map[String, ByteString] = {
    val map = new ju.HashMap[String, ByteString]()
    val hIter = headers.get.iterator()
    val colIter = elem.iterator()
    while (hIter.hasNext && colIter.hasNext) {
      map.put(hIter.next(), colIter.next())
    }
    map
  }
}
