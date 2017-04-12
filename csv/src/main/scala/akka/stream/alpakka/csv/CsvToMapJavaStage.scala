/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.csv

import java.nio.charset.Charset
import java.util.stream.Collectors
import java.{util => ju}

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream._
import akka.util.ByteString

/**
 * Internal Java API: Converts incoming {@link Collection}<{@link ByteString}> to {@link java.util.Map}<String, ByteString>.
 *
 * @param columnNames If given, these names are used as map keys; if not first stream element is used
 * @param charset     Character set used to convert header line ByteString to String
 */
private[csv] class CsvToMapJavaStage(columnNames: ju.Optional[ju.Collection[String]], charset: Charset)
    extends GraphStage[FlowShape[ju.Collection[ByteString], ju.Map[String, ByteString]]] {

  override protected def initialAttributes: Attributes = Attributes.name("CsvToMap")

  private val in = Inlet[ju.Collection[ByteString]]("CsvToMap.in")
  private val out = Outlet[ju.Map[String, ByteString]]("CsvToMap.out")
  override val shape = FlowShape.of(in, out)

  private final val decodeByteString = new java.util.function.Function[ByteString, String]() {
    override def apply(t: ByteString): String = t.decodeString(charset)
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private[this] var headers = columnNames

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val elem = grab(in)
            if (headers.isPresent) {
              val map = zipWithHeaders(elem)
              push(out, map)
            } else {
              headers = ju.Optional.of(decode(elem))
              pull(in)
            }
          }
        }
      )

      setHandler(out, new OutHandler {
        override def onPull(): Unit = pull(in)
      })

      private def decode(elem: ju.Collection[ByteString]) =
        elem.stream().map[String](decodeByteString).collect(Collectors.toList())

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

}
