/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb.javadsl

import akka.NotUsed
import akka.stream.alpakka.influxdb.{InfluxDbSettings, InfluxDbWriteMessage, InfluxDbWriteResult}
import org.influxdb.InfluxDB
import akka.stream.javadsl.Flow
import akka.stream.alpakka.influxdb.scaladsl
import org.influxdb.dto.Point

object InfluxDbFlow {

  def create(
      settings: InfluxDbSettings,
      influxDB: InfluxDB
  ): Flow[InfluxDbWriteMessage[Point, NotUsed], InfluxDbWriteResult[Point, NotUsed], NotUsed] =
    scaladsl.InfluxDbFlow.create(settings)(influxDB).asJava

  def typed[T](
      clazz: Class[T],
      settings: InfluxDbSettings,
      influxDB: InfluxDB
  ): Flow[InfluxDbWriteMessage[T, NotUsed], InfluxDbWriteResult[T, NotUsed], NotUsed] =
    scaladsl.InfluxDbFlow.typed(clazz, settings)(influxDB).asJava

  def createWithPassThrough[C](
      settings: InfluxDbSettings,
      influxDB: InfluxDB
  ): Flow[InfluxDbWriteMessage[Point, C], InfluxDbWriteResult[Point, C], NotUsed] =
    scaladsl.InfluxDbFlow.createWithPassThrough(settings)(influxDB).asJava

  def typedWithPassThrough[T, C](
      clazz: Class[T],
      settings: InfluxDbSettings,
      influxDB: InfluxDB
  ): Flow[InfluxDbWriteMessage[T, C], InfluxDbWriteResult[T, C], NotUsed] =
    scaladsl.InfluxDbFlow.typedWithPassThrough(clazz, settings)(influxDB).asJava

  def createWithContext[C](
      settings: InfluxDbSettings,
      influxDB: InfluxDB
  ): Flow[(InfluxDbWriteMessage[Point, NotUsed], C), (InfluxDbWriteResult[Point, C], C), NotUsed] =
    scaladsl.InfluxDbFlow.createWithContext(settings)(influxDB).asJava

  def typedWithContext[T, C](
      clazz: Class[T],
      settings: InfluxDbSettings,
      influxDB: InfluxDB
  ): Flow[(InfluxDbWriteMessage[T, NotUsed], C), (InfluxDbWriteResult[T, C], C), NotUsed] =
    scaladsl.InfluxDbFlow.typedWithContext(clazz, settings)(influxDB).asJava

}
