/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb.impl

import akka.stream._
import akka.stream.alpakka.influxdb.{InfluxDBWriteMessage, InfluxDBWriteResult}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import org.influxdb.InfluxDB
import org.influxdb.impl.InfluxDBResultMapperHelper

import scala.collection.immutable
import org.influxdb.BatchOptions
import org.influxdb.dto.{BatchPoints, Point}

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

  override protected def initialAttributes: Attributes =
    super.initialAttributes and Attributes(ActorAttributes.IODispatcher)

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
        val writtenMessages = messages.map(m => new InfluxDBWriteResult(m, None))
        influxDB.close()
        push(out, writtenMessages)
      }

      tryPull(in)
    }
  }

  final class InfluxDBRecordLogic extends InfluxDBLogic {

    override protected def write(messages: immutable.Seq[InfluxDBWriteMessage[T, C]]): Unit =
      messages
        .filter {
          case InfluxDBWriteMessage(_: Point, _, _, _) => {
            true
          }
          case InfluxDBWriteMessage(_: AnyRef, _, _, _) => {
            failStage(new RuntimeException(s"unexpected type Point required"))
            false
          }
        }
        .groupBy(im => (im.databaseName, im.retentionPolicy))
        .map(wm => toBatchPoints(wm._1._1, wm._1._2, wm._2))
        .foreach(influxDB.write)

  }

  final class InfluxDBMapperRecordLogic extends InfluxDBLogic {

    private val mapperHelper: InfluxDBResultMapperHelper = new InfluxDBResultMapperHelper

    override protected def write(messages: immutable.Seq[InfluxDBWriteMessage[T, C]]): Unit =
      messages
        .groupBy(groupByDbRp)
        .map(convertToBatchPoints)
        .foreach(influxDB.write)

    def groupByDbRp(im: InfluxDBWriteMessage[T, C]) =
      (
        im.databaseName match {
          case Some(databaseName) => Some(databaseName)
          case None => Some(mapperHelper.databaseName(im.point))
        },
        im.retentionPolicy match {
          case Some(databaseName) => Some(databaseName)
          case None => Some(mapperHelper.retentionPolicy(im.point))
        }
      )

    def convertToBatchPoints(wm: ((Some[String], Some[String]), immutable.Seq[InfluxDBWriteMessage[T, C]])) =
      toBatchPoints(wm._1._1,
                    wm._1._2,
                    wm._2.map(im => im.withPoint(mapperHelper.convertModelToPoint(im.point).asInstanceOf[T])))
  }

  private def toBatchPoints(databaseName: Option[String],
                            retentionPolicy: Option[String],
                            seq: Seq[InfluxDBWriteMessage[T, C]]) = {

    val builder = databaseName match {
      case Some(databaseName) => BatchPoints.database(databaseName)
      case None => BatchPoints.builder()
    }

    if (retentionPolicy.isDefined) builder.retentionPolicy(retentionPolicy.get)

    def convert(messages: Seq[InfluxDBWriteMessage[T, C]]): BatchPoints =
      messages match {
        case head :: tail => {
          builder.point(head.point.asInstanceOf[Point])
          convert(tail)
        }
        case Nil => builder.build()
      }

    convert(seq)
  }

}
