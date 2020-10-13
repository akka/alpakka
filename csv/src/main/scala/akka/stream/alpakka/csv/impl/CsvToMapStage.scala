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
 * @see akka.stream.alpakka.csv.impl.CsvToMapJavaStage
 *
 * @param columnNames If given, these names are used as map keys; if not first stream element is used
 * @param charset Character set used to convert header line ByteString to String
 */
@InternalApi private[csv] abstract class CsvToMapStageBase[V](columnNames: Option[immutable.Seq[String]],
                                                              charset: Charset
) extends GraphStage[FlowShape[immutable.Seq[ByteString], Map[String, V]]] {

  override protected def initialAttributes: Attributes = Attributes.name("CsvToMap")

  private val in = Inlet[immutable.Seq[ByteString]]("CsvToMap.in")
  private val out = Outlet[Map[String, V]]("CsvToMap.out")
  override val shape = FlowShape.of(in, out)

  protected def transformElements(elements: immutable.Seq[ByteString]): immutable.Seq[V]

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private var headers = columnNames

      setHandlers(in, out, this)

      override def onPush(): Unit = {
        val elem = grab(in)
        if (headers.isDefined) {
          val map = headers.get.zip(transformElements(elem)).toMap
          push(out, map)
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
@InternalApi private[csv] class CsvToMapStage(columnNames: Option[immutable.Seq[String]], charset: Charset)
    extends CsvToMapStageBase[ByteString](columnNames, charset) {

  override protected def transformElements(elements: immutable.Seq[ByteString]): immutable.Seq[ByteString] = elements
}

/**
 * Internal API
 */
@InternalApi private[csv] class CsvToMapAsStringsStage(columnNames: Option[immutable.Seq[String]], charset: Charset)
    extends CsvToMapStageBase[String](columnNames, charset) {

  override protected def transformElements(elements: immutable.Seq[ByteString]): immutable.Seq[String] =
    elements.map(_.decodeString(charset))
}
