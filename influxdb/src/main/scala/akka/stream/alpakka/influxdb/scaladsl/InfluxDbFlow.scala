/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb.scaladsl

import akka.NotUsed
import akka.stream.alpakka.influxdb.{impl, InfluxDbSettings, InfluxDbWriteMessage, InfluxDbWriteResult}
import akka.stream.scaladsl.Flow
import org.influxdb.InfluxDB
import org.influxdb.dto.Point

import scala.collection.immutable

/**
 * Scala API to create InfluxDB flows.
 */
object InfluxDbFlow {

  def create(settings: InfluxDbSettings)(
      implicit influxDB: InfluxDB
  ): Flow[InfluxDbWriteMessage[Point, NotUsed], InfluxDbWriteResult[Point, NotUsed], NotUsed] =
    Flow[InfluxDbWriteMessage[Point, NotUsed]]
      .batch(settings.batchSize, immutable.Seq(_))(_ :+ _)
      .via(new impl.InfluxDbFlowStage[NotUsed](influxDB))
      .mapConcat(identity)

  def typed[T](clazz: Class[T], settings: InfluxDbSettings)(
      implicit influxDB: InfluxDB
  ): Flow[InfluxDbWriteMessage[T, NotUsed], InfluxDbWriteResult[T, NotUsed], NotUsed] =
    Flow[InfluxDbWriteMessage[T, NotUsed]]
      .batch(settings.batchSize, immutable.Seq(_))(_ :+ _)
      .via(new impl.InfluxDbMapperFlowStage[T, NotUsed](clazz, influxDB))
      .mapConcat(identity)

  def createWithPassThrough[C](settings: InfluxDbSettings)(
      implicit influxDB: InfluxDB
  ): Flow[InfluxDbWriteMessage[Point, C], InfluxDbWriteResult[Point, C], NotUsed] =
    Flow[InfluxDbWriteMessage[Point, C]]
      .batch(settings.batchSize, immutable.Seq(_))(_ :+ _)
      .via(new impl.InfluxDbFlowStage[C](influxDB))
      .mapConcat(identity)

  def typedWithPassThrough[T, C](clazz: Class[T], settings: InfluxDbSettings)(
      implicit influxDB: InfluxDB
  ): Flow[InfluxDbWriteMessage[T, C], InfluxDbWriteResult[T, C], NotUsed] =
    Flow[InfluxDbWriteMessage[T, C]]
      .batch(settings.batchSize, immutable.Seq(_))(_ :+ _)
      .via(new impl.InfluxDbMapperFlowStage[T, C](clazz, influxDB))
      .mapConcat(identity)

  def createWithContext[C](settings: InfluxDbSettings)(
      implicit influxDB: InfluxDB
  ): Flow[(InfluxDbWriteMessage[Point, NotUsed], C), (InfluxDbWriteResult[Point, C], C), NotUsed] =
    Flow[(InfluxDbWriteMessage[Point, NotUsed], C)]
      .map {
        case (wm, pt) => wm.withPassThrough(pt)
      }
      .via(
        createWithPassThrough(settings)
      )
      .map { wr =>
        (wr, wr.writeMessage.passThrough)
      }

  def typedWithContext[T, C](clazz: Class[T], settings: InfluxDbSettings)(
      implicit influxDB: InfluxDB
  ): Flow[(InfluxDbWriteMessage[T, NotUsed], C), (InfluxDbWriteResult[T, C], C), NotUsed] =
    Flow[(InfluxDbWriteMessage[T, NotUsed], C)]
      .map {
        case (wm, pt) => wm.withPassThrough(pt)
      }
      .via(typedWithPassThrough(clazz, settings))
      .map { wr =>
        (wr, wr.writeMessage.passThrough)
      }

}
