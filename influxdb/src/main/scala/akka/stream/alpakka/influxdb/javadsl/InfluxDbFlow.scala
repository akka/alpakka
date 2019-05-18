/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb.javadsl

import akka.NotUsed
import akka.stream.alpakka.influxdb.{InfluxDbSettings, InfluxDbWriteMessage, InfluxDbWriteResult}
import org.influxdb.InfluxDB
import akka.stream.javadsl.Flow
import akka.stream.alpakka.influxdb.scaladsl

object InfluxDbFlow {

  def create[T](
      settings: InfluxDbSettings,
      influxDB: InfluxDB
  ): Flow[InfluxDbWriteMessage[T, NotUsed], InfluxDbWriteResult[T, NotUsed], NotUsed] =
    scaladsl.InfluxDbFlow.create(settings)(influxDB).asJava

  def typed[T](
      clazz: Class[T],
      settings: InfluxDbSettings,
      influxDB: InfluxDB
  ): Flow[InfluxDbWriteMessage[T, NotUsed], InfluxDbWriteResult[T, NotUsed], NotUsed] =
    scaladsl.InfluxDbFlow.typed(clazz, settings)(influxDB).asJava

  def createWithPassThrough[T, C](
      settings: InfluxDbSettings,
      influxDB: InfluxDB
  ): Flow[InfluxDbWriteMessage[T, C], InfluxDbWriteResult[T, C], NotUsed] =
    scaladsl.InfluxDbFlow.createWithPassThrough(settings)(influxDB).asJava

  def typedWithPassThrough[T, C](
      clazz: Class[T],
      settings: InfluxDbSettings,
      influxDB: InfluxDB
  ): Flow[InfluxDbWriteMessage[T, C], InfluxDbWriteResult[T, C], NotUsed] =
    scaladsl.InfluxDbFlow.typedWithPassThrough(clazz, settings)(influxDB).asJava

  def createWithContext[T, C](
      settings: InfluxDbSettings,
      influxDB: InfluxDB
  ): Flow[(InfluxDbWriteMessage[T, NotUsed], C), (InfluxDbWriteResult[T, C], C), NotUsed] =
    scaladsl.InfluxDbFlow.createWithContext(settings)(influxDB).asJava

  def typedWithContext[T, C](
      clazz: Class[T],
      settings: InfluxDbSettings,
      influxDB: InfluxDB
  ): Flow[(InfluxDbWriteMessage[T, NotUsed], C), (InfluxDbWriteResult[T, C], C), NotUsed] =
    scaladsl.InfluxDbFlow.typedWithContext(clazz, settings)(influxDB).asJava

}
