/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb.impl

import akka.stream._
import akka.stream.alpakka.influxdb.{InfluxDbWriteMessage, InfluxDbWriteResult}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import org.influxdb.InfluxDB

import scala.collection.immutable
import org.influxdb.BatchOptions
import org.influxdb.dto.{BatchPoints, Point}
import org.influxdb.impl.InfluxDbResultMapperHelper

/**
 * INTERNAL API
 */
private[influxdb] class InfluxDbFlowStage[T, C](
    clazz: Option[Class[T]],
    influxDB: InfluxDB
) extends GraphStage[FlowShape[immutable.Seq[InfluxDbWriteMessage[T, C]], immutable.Seq[InfluxDbWriteResult[T, C]]]] {
  private val in = Inlet[immutable.Seq[InfluxDbWriteMessage[T, C]]]("in")
  private val out = Outlet[immutable.Seq[InfluxDbWriteResult[T, C]]]("out")

  override val shape = FlowShape(in, out)

  override protected def initialAttributes: Attributes =
    super.initialAttributes and Attributes(ActorAttributes.IODispatcher)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    clazz match {
      case Some(c) => new InfluxDbMapperRecordLogic
      case None => new InfluxDbRecordLogic
    }

  sealed abstract class InfluxDbLogic extends GraphStageLogic(shape) with InHandler with OutHandler {

    protected def write(messages: immutable.Seq[InfluxDbWriteMessage[T, C]]): Unit

    setHandlers(in, out, this)

    override def onPull(): Unit = if (!isClosed(in) && !hasBeenPulled(in)) pull(in)

    override def onPush(): Unit = {
      val messages = grab(in)
      if (messages.nonEmpty) {

        influxDB.enableBatch(BatchOptions.DEFAULTS)
        write(messages)
        val writtenMessages = messages.map(m => new InfluxDbWriteResult(m, None))
        influxDB.close()
        push(out, writtenMessages)
      }

      tryPull(in)
    }
  }

  final class InfluxDbRecordLogic extends InfluxDbLogic {

    override protected def write(messages: immutable.Seq[InfluxDbWriteMessage[T, C]]): Unit =
      messages
        .filter {
          case InfluxDbWriteMessage(_: Point, _, _, _) => {
            true
          }
          case InfluxDbWriteMessage(_: AnyRef, _, _, _) => {
            failStage(new RuntimeException(s"unexpected type Point required"))
            false
          }
        }
        .groupBy(im => (im.databaseName, im.retentionPolicy))
        .map(wm => toBatchPoints(wm._1._1, wm._1._2, wm._2))
        .foreach(influxDB.write)

  }

  final class InfluxDbMapperRecordLogic extends InfluxDbLogic {

    private val mapperHelper: InfluxDbResultMapperHelper = new InfluxDbResultMapperHelper

    override protected def write(messages: immutable.Seq[InfluxDbWriteMessage[T, C]]): Unit =
      messages
        .groupBy(groupByDbRp)
        .map(convertToBatchPoints)
        .foreach(influxDB.write)

    def groupByDbRp(im: InfluxDbWriteMessage[T, C]) =
      (
        im.databaseName match {
          case dbn: Some[String] => dbn
          case None => Some(mapperHelper.databaseName(im.point))
        },
        im.retentionPolicy match {
          case dbn: Some[String] => dbn
          case None => Some(mapperHelper.retentionPolicy(im.point))
        }
      )

    def convertToBatchPoints(wm: ((Some[String], Some[String]), immutable.Seq[InfluxDbWriteMessage[T, C]])) =
      toBatchPoints(wm._1._1,
                    wm._1._2,
                    wm._2.map(im => im.withPoint(mapperHelper.convertModelToPoint(im.point).asInstanceOf[T])))
  }

  private def toBatchPoints(databaseName: Option[String],
                            retentionPolicy: Option[String],
                            seq: Seq[InfluxDbWriteMessage[T, C]]) = {

    val builder = databaseName match {
      case Some(databaseName) => BatchPoints.database(databaseName)
      case None => BatchPoints.builder()
    }

    if (retentionPolicy.isDefined) builder.retentionPolicy(retentionPolicy.get)

    def convert(messages: Seq[InfluxDbWriteMessage[T, C]]): BatchPoints =
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
