/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb.scaladsl

import akka.NotUsed
import akka.stream.alpakka.influxdb.{impl, InfluxDbSettings, InfluxDbWriteMessage, InfluxDbWriteResult}
import akka.stream.scaladsl.Flow
import org.influxdb.InfluxDB

import scala.collection.immutable

/**
 * Scala API to create InfluxDB flows.
 */
object InfluxDbFlow {

  def create[T](settings: InfluxDbSettings)(
      implicit influxDB: InfluxDB
  ): Flow[InfluxDbWriteMessage[T, NotUsed], InfluxDbWriteResult[T, NotUsed], NotUsed] =
    Flow[InfluxDbWriteMessage[T, NotUsed]]
      .batch(settings.batchSize, immutable.Seq(_))(_ :+ _)
      .via(new impl.InfluxDbFlowStage[T, NotUsed](None, influxDB))
      .mapConcat(identity)

  def typed[T](clazz: Class[T], settings: InfluxDbSettings)(
      implicit influxDB: InfluxDB
  ): Flow[InfluxDbWriteMessage[T, NotUsed], InfluxDbWriteResult[T, NotUsed], NotUsed] =
    Flow[InfluxDbWriteMessage[T, NotUsed]]
      .batch(settings.batchSize, immutable.Seq(_))(_ :+ _)
      .via(new impl.InfluxDbFlowStage[T, NotUsed](Some(clazz), influxDB))
      .mapConcat(identity)

  def createWithPassThrough[T, C](settings: InfluxDbSettings)(
      implicit influxDB: InfluxDB
  ): Flow[InfluxDbWriteMessage[T, C], InfluxDbWriteResult[T, C], NotUsed] =
    Flow[InfluxDbWriteMessage[T, C]]
      .batch(settings.batchSize, immutable.Seq(_))(_ :+ _)
      .via(new impl.InfluxDbFlowStage[T, C](None, influxDB))
      .mapConcat(identity)

  def typedWithPassThrough[T, C](clazz: Class[T], settings: InfluxDbSettings)(
      implicit influxDB: InfluxDB
  ): Flow[InfluxDbWriteMessage[T, C], InfluxDbWriteResult[T, C], NotUsed] =
    Flow[InfluxDbWriteMessage[T, C]]
      .batch(settings.batchSize, immutable.Seq(_))(_ :+ _)
      .via(new impl.InfluxDbFlowStage[T, C](Some(clazz), influxDB))
      .mapConcat(identity)

  def createWithContext[T, C](settings: InfluxDbSettings)(
      implicit influxDB: InfluxDB
  ): Flow[(InfluxDbWriteMessage[T, NotUsed], C), (InfluxDbWriteResult[T, C], C), NotUsed] =
    Flow[(InfluxDbWriteMessage[T, NotUsed], C)]
      .map {
        case (wm, pt) =>
          InfluxDbWriteMessage(wm.point, pt)
      }
      .via(createWithPassThrough(settings))
      .map { wr =>
        (wr, wr.writeMessage.passThrough)
      }

  def typedWithContext[T, C](clazz: Class[T], settings: InfluxDbSettings)(
      implicit influxDB: InfluxDB
  ): Flow[(InfluxDbWriteMessage[T, NotUsed], C), (InfluxDbWriteResult[T, C], C), NotUsed] =
    Flow[(InfluxDbWriteMessage[T, NotUsed], C)]
      .map {
        case (wm, pt) =>
          InfluxDbWriteMessage(wm.point, pt)
      }
      .via(typedWithPassThrough(clazz, settings))
      .map { wr =>
        (wr, wr.writeMessage.passThrough)
      }

}
