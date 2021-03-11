/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.csv.impl

import java.nio.charset.Charset
import akka.annotation.InternalApi
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString

import scala.collection.immutable

/**
 * Internal API: Converts incoming [[List[ByteString]]] to [[Map[String, ByteString]]].
 *
 * @see akka.stream.alpakka.csv.impl.CsvToMapJavaStage
 * @param columnNames If given, these names are used as map keys; if not first stream element is used
 * @param charset Character set used to convert header line ByteString to String
 * @param combineAll If true, placeholder elements will be used to extend the shorter collection to the length of the longer.
 * @param headerPlaceholder placeholder used when there are more headers than data.
 */
@InternalApi private[csv] abstract class CsvToMapStageBase[V](columnNames: Option[immutable.Seq[String]],
                                                              charset: Charset,
                                                              combineAll: Boolean,
                                                              customFieldValuePlaceHolder: Option[V],
                                                              headerPlaceholder: Option[String])
    extends GraphStage[FlowShape[immutable.Seq[ByteString], Map[String, V]]] {

  override protected def initialAttributes: Attributes = Attributes.name("CsvToMap")

  private type Headers = Option[Seq[String]]

  private val in = Inlet[immutable.Seq[ByteString]]("CsvToMap.in")
  private val out = Outlet[Map[String, V]]("CsvToMap.out")
  override val shape = FlowShape.of(in, out)

  val fieldValuePlaceholder: V

  protected def transformElements(elements: immutable.Seq[ByteString]): immutable.Seq[V]

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private var headers = columnNames

      setHandlers(in, out, this)

      override def onPush(): Unit = {
        val elem = grab(in)
        if (combineAll) {
          process(elem, combineUsingPlaceholder(elem))
        } else {
          process(elem, headers => headers.get.zip(transformElements(elem)).toMap)
        }
      }

      private def process(elem: immutable.Seq[ByteString], combine: => Headers => Map[String, V]): Unit = {
        if (headers.isDefined) {
          push(out, combine(headers))
        } else {
          headers = Some(elem.map(_.decodeString(charset)))
          pull(in)
        }
      }

      override def onPull(): Unit = pull(in)
    }

  private def combineUsingPlaceholder(elem: immutable.Seq[ByteString]): Headers => Map[String, V] = headers => {
    val combined = headers.get
      .zipAll(transformElements(elem),
              headerPlaceholder.getOrElse("MissingHeader"),
              customFieldValuePlaceHolder.getOrElse(fieldValuePlaceholder))
    val filtering: String => Boolean = key =>
      headerPlaceholder.map(_.equalsIgnoreCase(key)).fold(key.equalsIgnoreCase("MissingHeader"))(identity)
    val missingHeadersContent =
      combined
        .filter {
          case (key, _) => filtering(key)
        }
        .zipWithIndex
        .map {
          case (content, index) =>
            val (key, value) = content
            (s"$key$index", value)
        }
    val contentWithPredefinedHeaders =
      combined
        .filterNot {
          case (key, _) =>
            filtering(key)
        }
    val all = contentWithPredefinedHeaders ++ missingHeadersContent
    all.toMap
  }
}

/**
 * Internal API
 */
@InternalApi private[csv] class CsvToMapStage(columnNames: Option[immutable.Seq[String]],
                                              charset: Charset,
                                              combineAll: Boolean,
                                              customFieldValuePlaceHolder: Option[ByteString],
                                              headerPlaceholder: Option[String])
    extends CsvToMapStageBase[ByteString](columnNames,
                                          charset,
                                          combineAll,
                                          customFieldValuePlaceHolder,
                                          headerPlaceholder) {

  override val fieldValuePlaceholder: ByteString = ByteString("")

  override protected def transformElements(elements: immutable.Seq[ByteString]): immutable.Seq[ByteString] = elements

}

/**
 * Internal API
 */
@InternalApi private[csv] class CsvToMapAsStringsStage(columnNames: Option[immutable.Seq[String]],
                                                       charset: Charset,
                                                       combineAll: Boolean,
                                                       customFieldValuePlaceHolder: Option[String],
                                                       headerPlaceholder: Option[String])
    extends CsvToMapStageBase[String](columnNames, charset, combineAll, customFieldValuePlaceHolder, headerPlaceholder) {

  override val fieldValuePlaceholder: String = ""

  override protected def transformElements(elements: immutable.Seq[ByteString]): immutable.Seq[String] =
    elements.map(_.decodeString(charset))
}
