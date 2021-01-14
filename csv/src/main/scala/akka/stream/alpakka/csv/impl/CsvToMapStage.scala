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
 * If the headers list is larger than the values attached to those headers, an empty String will be set by default to the orphan header
 * If the values list is larger than the headers list, the user can configure a default value for the missing header
 *
 * @see akka.stream.alpakka.csv.impl.CsvToMapJavaStage
 * @param columnNames If given, these names are used as map keys; if not first stream element is used
 * @param charset Character set used to convert header line ByteString to String
 *@param combineAll If true, placeholder elements will be used to extend the shorter collection to the length of the longer.
 */
@InternalApi private[csv] abstract class CsvToMapStageBase[V](columnNames: Option[immutable.Seq[String]],
                                                              charset: Charset,
                                                              combineAll: Boolean)
    extends GraphStage[FlowShape[immutable.Seq[ByteString], Map[String, V]]] {

  override protected def initialAttributes: Attributes = Attributes.name("CsvToMap")

  private type Headers = Option[Seq[String]]

  private val in = Inlet[immutable.Seq[ByteString]]("CsvToMap.in")
  private val out = Outlet[Map[String, V]]("CsvToMap.out")
  val fieldValuePlaceholder: V
  override val shape = FlowShape.of(in, out)

  protected def calculateDefaultHeaderValue(): String

  protected def transformElements(elements: immutable.Seq[ByteString]): immutable.Seq[V]

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private var headers = columnNames

      setHandlers(in, out, this)

      override def onPush(): Unit = {
        val elem = grab(in)
        if (combineAll) {
          val combiner: Option[Seq[String]] => Map[String, V] = headers =>
            headers.get
              .zipAll(transformElements(elem), calculateDefaultHeaderValue(), fieldValuePlaceholder)
              .toMap
          process(elem, combiner)
        } else {
          process(elem, headers => headers.get.zip(transformElements(elem)).toMap)
        }
      }

      private def process(elem: immutable.Seq[ByteString], combiner: Headers => Map[String, V]): Unit = {
        if (headers.isDefined) {
          push(out, combiner(headers))
        } else {
          headers = Some(elem.map(_.decodeString(charset)))
          pull(in)
        }
      }

      override def onPull(): Unit = pull(in)
    }
}

/**
 * Internal API
 */
@InternalApi private[csv] class CsvToMapStage(columnNames: Option[immutable.Seq[String]],
                                              charset: Charset,
                                              combineAll: Boolean,
                                              headerPlaceholder: Option[String])
    extends CsvToMapStageBase[ByteString](columnNames, charset, combineAll) {
  override val fieldValuePlaceholder: ByteString = ByteString("")

  override protected def transformElements(elements: immutable.Seq[ByteString]): immutable.Seq[ByteString] = elements

  override protected def calculateDefaultHeaderValue(): String =
    headerPlaceholder.fold("Missing Header")(identity)

}

/**
 * Internal API
 */
@InternalApi private[csv] class CsvToMapAsStringsStage(columnNames: Option[immutable.Seq[String]],
                                                       charset: Charset,
                                                       combineAll: Boolean,
                                                       headerPlaceholder: Option[String])
    extends CsvToMapStageBase[String](columnNames, charset, combineAll) {

  override val fieldValuePlaceholder: String = ""

  override protected def transformElements(elements: immutable.Seq[ByteString]): immutable.Seq[String] =
    elements.map(_.decodeString(charset))

  override protected def calculateDefaultHeaderValue(): String =
    headerPlaceholder.fold("Missing Header")(identity)
}
