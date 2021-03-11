/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.csv.impl

import java.nio.charset.Charset
import java.util.stream.Collectors
import java.{util => ju}
import akka.annotation.InternalApi
import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString

/**
 * Internal Java API: Converts incoming {@link Collection}<{@link ByteString}> to {@link java.util.Map}<String, ByteString>.
 *
 * @param columnNames If given, these names are used as map keys; if not first stream element is used
 * @param charset Character set used to convert header line ByteString to String
 * @param combineAll If true, placeholder elements will be used to extend the shorter collection to the length of the longer.
 * @param customFieldValuePlaceholder placeholder used when there are more data than headers.
 * @param headerPlaceholder placeholder used when there are more headers than data.
 */
@InternalApi private[csv] abstract class CsvToMapJavaStageBase[V](columnNames: ju.Optional[ju.Collection[String]],
                                                                  charset: Charset,
                                                                  combineAll: Boolean,
                                                                  customFieldValuePlaceholder: ju.Optional[V],
                                                                  headerPlaceholder: ju.Optional[String])
    extends GraphStage[FlowShape[ju.Collection[ByteString], ju.Map[String, V]]] {

  override protected def initialAttributes: Attributes = Attributes.name("CsvToMap")

  private val in = Inlet[ju.Collection[ByteString]]("CsvToMap.in")
  private val out = Outlet[ju.Map[String, V]]("CsvToMap.out")
  override val shape = FlowShape.of(in, out)

  val fieldValuePlaceholder: V

  protected def transformElements(elements: ju.Collection[ByteString]): ju.Collection[V]

  private final val decodeByteString = new java.util.function.Function[ByteString, String]() {
    override def apply(t: ByteString): String = t.decodeString(charset)
  }

  protected def decode(elem: ju.Collection[ByteString]): ju.List[String] =
    elem.stream().map[String](decodeByteString).collect(Collectors.toList())

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private[this] var headers = columnNames

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val elem = grab(in)
            if (combineAll) {
              process(elem, zipAllWithHeaders)
            } else {
              process(elem, zipWithHeaders)
            }
          }
        }
      )

      private def process(elem: ju.Collection[ByteString], combine: ju.Collection[V] => ju.Map[String, V]) = {
        if (headers.isPresent) {
          val map = combine(transformElements(elem))
          push(out, map)
        } else {
          headers = ju.Optional.of(decode(elem))
          pull(in)
        }
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit = pull(in)
      })

      private def zipWithHeaders(elem: ju.Collection[V]): ju.Map[String, V] = {
        val map = new ju.HashMap[String, V]()
        val hIter = headers.get.iterator()
        val colIter = elem.iterator()
        while (hIter.hasNext && colIter.hasNext) {
          map.put(hIter.next(), colIter.next())
        }
        map
      }

      private def zipAllWithHeaders(elem: ju.Collection[V]): ju.Map[String, V] = {
        val map = new ju.HashMap[String, V]()
        val hIter = headers.get.iterator()
        val colIter = elem.iterator()
        if (headers.get.size() > elem.size()) {
          while (hIter.hasNext) {
            if (colIter.hasNext) {
              map.put(hIter.next(), colIter.next())
            } else {
              map.put(hIter.next(), customFieldValuePlaceholder.orElse(fieldValuePlaceholder))
            }
          }
        } else if (elem.size() > headers.get.size()) {
          var index = 0
          while (colIter.hasNext) {
            if (hIter.hasNext) {
              map.put(hIter.next(), colIter.next())
            } else {
              map.put(headerPlaceholder.orElse("MissingHeader") + index, colIter.next())
              index = index + 1
            }
          }

        } else {
          while (hIter.hasNext && colIter.hasNext) {
            map.put(hIter.next(), colIter.next())
          }
        }
        map
      }

    }

}

/**
 * Internal API
 */
@InternalApi private[csv] class CsvToMapJavaStage(columnNames: ju.Optional[ju.Collection[String]],
                                                  charset: Charset,
                                                  combineAll: Boolean,
                                                  customFieldValuePlaceholder: ju.Optional[ByteString],
                                                  headerPlaceholder: ju.Optional[String])
    extends CsvToMapJavaStageBase[ByteString](columnNames,
                                              charset,
                                              combineAll,
                                              customFieldValuePlaceholder,
                                              headerPlaceholder) {

  override val fieldValuePlaceholder: ByteString = ByteString("")

  override protected def transformElements(elements: ju.Collection[ByteString]): ju.Collection[ByteString] =
    elements
}

/**
 * Internal API
 */
@InternalApi private[csv] class CsvToMapAsStringsJavaStage(columnNames: ju.Optional[ju.Collection[String]],
                                                           charset: Charset,
                                                           combineAll: Boolean,
                                                           customFieldValuePlaceholder: ju.Optional[String],
                                                           headerPlaceholder: ju.Optional[String])
    extends CsvToMapJavaStageBase[String](columnNames,
                                          charset,
                                          combineAll,
                                          customFieldValuePlaceholder,
                                          headerPlaceholder) {

  override val fieldValuePlaceholder: String = ""

  override protected def transformElements(elements: ju.Collection[ByteString]): ju.Collection[String] =
    decode(elements)
}
