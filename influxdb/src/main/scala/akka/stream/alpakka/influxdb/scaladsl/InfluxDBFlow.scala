/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb.scaladsl

import akka.NotUsed
import akka.stream.alpakka.influxdb.{impl, InfluxDBSettings, InfluxDBWriteMessage, InfluxDBWriteResult}
import akka.stream.scaladsl.Flow
import org.influxdb.InfluxDB

import scala.collection.immutable

/**
 * Scala API to create InfluxDB flows.
 */
object InfluxDBFlow {

  def create[T](settings: InfluxDBSettings)(
      implicit influxDB: InfluxDB
  ): Flow[InfluxDBWriteMessage[T, NotUsed], InfluxDBWriteResult[T, NotUsed], NotUsed] =
    Flow[InfluxDBWriteMessage[T, NotUsed]]
      .batch(settings.batchSize, immutable.Seq(_)) { case (seq, wm) => seq :+ wm }
      .via(
        new impl.InfluxDBFlowStage[T, NotUsed](
          Option.empty,
          influxDB
        )
      )
      .mapConcat(identity)

  def typed[T](clazz: Class[T], settings: InfluxDBSettings)(
      implicit influxDB: InfluxDB
  ): Flow[InfluxDBWriteMessage[T, NotUsed], InfluxDBWriteResult[T, NotUsed], NotUsed] =
    Flow[InfluxDBWriteMessage[T, NotUsed]]
      .batch(settings.batchSize, immutable.Seq(_)) { case (seq, wm) => seq :+ wm }
      .via(
        new impl.InfluxDBFlowStage[T, NotUsed](
          Option(clazz),
          influxDB
        )
      )
      .mapConcat(identity)

  def createWithPassThrough[T, C](settings: InfluxDBSettings)(
      implicit influxDB: InfluxDB
  ): Flow[InfluxDBWriteMessage[T, C], InfluxDBWriteResult[T, C], NotUsed] =
    Flow[InfluxDBWriteMessage[T, C]]
      .batch(settings.batchSize, immutable.Seq(_)) { case (seq, wm) => seq :+ wm }
      .via(
        new impl.InfluxDBFlowStage[T, C](
          Option.empty,
          influxDB
        )
      )
      .mapConcat(identity)

  def typedWithPassThrough[T, C](clazz: Class[T], settings: InfluxDBSettings)(
      implicit influxDB: InfluxDB
  ): Flow[InfluxDBWriteMessage[T, C], InfluxDBWriteResult[T, C], NotUsed] =
    Flow[InfluxDBWriteMessage[T, C]]
      .batch(settings.batchSize, immutable.Seq(_)) { case (seq, wm) => seq :+ wm }
      .via(
        new impl.InfluxDBFlowStage[T, C](
          Option(clazz),
          influxDB
        )
      )
      .mapConcat(identity)

  def createWithContext[T, C](settings: InfluxDBSettings)(
      implicit influxDB: InfluxDB
  ): Flow[(InfluxDBWriteMessage[T, NotUsed], C), (InfluxDBWriteResult[T, C], C), NotUsed] =
    Flow[(InfluxDBWriteMessage[T, NotUsed], C)]
      .map {
        case (wm, pt) =>
          InfluxDBWriteMessage(wm.point, pt)
      }
      .via(createWithPassThrough(settings))
      .map { wr =>
        (wr, wr.writeMessage.passThrough)
      }

  def typedWithContext[T, C](clazz: Class[T], settings: InfluxDBSettings)(
      implicit influxDB: InfluxDB
  ): Flow[(InfluxDBWriteMessage[T, NotUsed], C), (InfluxDBWriteResult[T, C], C), NotUsed] =
    Flow[(InfluxDBWriteMessage[T, NotUsed], C)]
      .map {
        case (wm, pt) =>
          InfluxDBWriteMessage(wm.point, pt)
      }
      .via(typedWithPassThrough(clazz, settings))
      .map { wr =>
        (wr, wr.writeMessage.passThrough)
      }

}
