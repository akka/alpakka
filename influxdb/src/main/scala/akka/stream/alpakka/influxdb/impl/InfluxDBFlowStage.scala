/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb.impl

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.alpakka.influxdb.{InfluxDBWriteMessage, InfluxDBWriteResult}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import org.influxdb.InfluxDB
import org.influxdb.impl.InfluxDBMapper

import scala.collection.immutable
import org.influxdb.BatchOptions
import org.influxdb.dto.Point

/**
 * INTERNAL API
 */
private[influxdb] class InfluxDBFlowStage[T, C](
    clazz: Option[Class[T]],
    influxDB: InfluxDB
) extends GraphStage[FlowShape[immutable.Seq[InfluxDBWriteMessage[T, C]], immutable.Seq[InfluxDBWriteResult[T, C]]]] {
  private val in = Inlet[immutable.Seq[InfluxDBWriteMessage[T, C]]]("in")
  private val out = Outlet[immutable.Seq[InfluxDBWriteResult[T, C]]]("out")

  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    clazz match {
      case Some(c) => new InfluxDBMapperRecordLogic
      case None => new InfluxDBRecordLogic
    }

  sealed abstract class InfluxDBLogic extends GraphStageLogic(shape) with InHandler with OutHandler {

    protected def write(messages: immutable.Seq[InfluxDBWriteMessage[T, C]]): Unit

    setHandlers(in, out, this)

    override def onPull(): Unit = if (!isClosed(in) && !hasBeenPulled(in)) pull(in)

    override def onPush(): Unit = {
      val messages = grab(in)
      if (messages.nonEmpty) {

        influxDB.enableBatch(BatchOptions.DEFAULTS)
        write(messages)
        val writtenMessages = messages.map(m => new InfluxDBWriteResult(m, Option.empty))
        influxDB.close()
        push(out, writtenMessages)
      }

      tryPull(in)
    }
  }

  final class InfluxDBRecordLogic extends InfluxDBLogic {

    override protected def write(messages: immutable.Seq[InfluxDBWriteMessage[T, C]]): Unit =
      messages.foreach {
        case InfluxDBWriteMessage(point: Point, _) => {
          influxDB.write(point)
          println(point.toString)
        }
        case InfluxDBWriteMessage(others: AnyRef, _) =>
          failStage(new RuntimeException(s"unexpected type Point or annotated with Measurement required"))
      }

  }

  final class InfluxDBMapperRecordLogic extends InfluxDBLogic {

    protected var influxDBMapper: InfluxDBMapper = _

    override def preStart(): Unit =
      influxDBMapper = new InfluxDBMapper(influxDB)

    override protected def write(messages: immutable.Seq[InfluxDBWriteMessage[T, C]]): Unit =
      messages.foreach {
        case InfluxDBWriteMessage(typeMetric: Any, _) =>
          influxDBMapper.save(typeMetric)
      }
  }

}
