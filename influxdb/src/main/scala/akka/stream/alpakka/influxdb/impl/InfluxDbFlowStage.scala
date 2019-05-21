/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb.impl

import akka.annotation.InternalApi
import akka.stream._
import akka.stream.alpakka.influxdb.{InfluxDbWriteMessage, InfluxDbWriteResult}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import org.influxdb.{BatchOptions, InfluxDB}

import scala.collection.immutable
import org.influxdb.dto.{BatchPoints, Point}
import org.influxdb.impl.AlpakkaResultMapperHelper

import scala.annotation.tailrec

/**
 * INTERNAL API
 */
@InternalApi
private[influxdb] class InfluxDbFlowStage[C](
    influxDB: InfluxDB
) extends GraphStage[
      FlowShape[immutable.Seq[InfluxDbWriteMessage[Point, C]], immutable.Seq[InfluxDbWriteResult[Point, C]]]
    ] {
  private val in = Inlet[immutable.Seq[InfluxDbWriteMessage[Point, C]]]("in")
  private val out = Outlet[immutable.Seq[InfluxDbWriteResult[Point, C]]]("out")

  override val shape = FlowShape(in, out)

  override protected def initialAttributes: Attributes =
    super.initialAttributes and Attributes(ActorAttributes.IODispatcher)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new InfluxDbRecordLogic(influxDB, in, out, shape)

}
private[influxdb] class InfluxDbMapperFlowStage[T, C](
    clazz: Class[T],
    influxDB: InfluxDB
) extends GraphStage[FlowShape[immutable.Seq[InfluxDbWriteMessage[T, C]], immutable.Seq[InfluxDbWriteResult[T, C]]]] {

  private val in = Inlet[immutable.Seq[InfluxDbWriteMessage[T, C]]]("in")
  private val out = Outlet[immutable.Seq[InfluxDbWriteResult[T, C]]]("out")

  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new InfluxDbMapperRecordLogic(influxDB, in, out, shape)

}

private[influxdb] sealed abstract class InfluxDbLogic[T, C](
    influxDB: InfluxDB,
    in: Inlet[immutable.Seq[InfluxDbWriteMessage[T, C]]],
    out: Outlet[immutable.Seq[InfluxDbWriteResult[T, C]]],
    shape: FlowShape[immutable.Seq[InfluxDbWriteMessage[T, C]], immutable.Seq[InfluxDbWriteResult[T, C]]]
) extends GraphStageLogic(shape)
    with InHandler
    with OutHandler {

  setHandlers(in, out, this)

  protected def write(messages: immutable.Seq[InfluxDbWriteMessage[T, C]]): Unit

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

  protected def toBatchPoints(databaseName: Option[String],
                              retentionPolicy: Option[String],
                              seq: Seq[InfluxDbWriteMessage[T, C]]) = {
    val builder = databaseName match {
      case Some(databaseName) => BatchPoints.database(databaseName)
      case None => BatchPoints.builder()
    }

    retentionPolicy.foreach(builder.retentionPolicy)

    @tailrec
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

private[influxdb] final class InfluxDbRecordLogic[C](
    influxDB: InfluxDB,
    in: Inlet[immutable.Seq[InfluxDbWriteMessage[Point, C]]],
    out: Outlet[immutable.Seq[InfluxDbWriteResult[Point, C]]],
    shape: FlowShape[immutable.Seq[InfluxDbWriteMessage[Point, C]], immutable.Seq[InfluxDbWriteResult[Point, C]]]
) extends InfluxDbLogic(influxDB, in, out, shape) {

  override protected def write(messages: immutable.Seq[InfluxDbWriteMessage[Point, C]]): Unit =
    messages
      .groupBy(im => (im.databaseName, im.retentionPolicy))
      .map(wm => toBatchPoints(wm._1._1, wm._1._2, wm._2))
      .foreach(influxDB.write)
}

private[influxdb] final class InfluxDbMapperRecordLogic[T, C](
    influxDB: InfluxDB,
    in: Inlet[immutable.Seq[InfluxDbWriteMessage[T, C]]],
    out: Outlet[immutable.Seq[InfluxDbWriteResult[T, C]]],
    shape: FlowShape[immutable.Seq[InfluxDbWriteMessage[T, C]], immutable.Seq[InfluxDbWriteResult[T, C]]]
) extends InfluxDbLogic(influxDB, in, out, shape) {

  private val mapperHelper: AlpakkaResultMapperHelper = new AlpakkaResultMapperHelper

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
