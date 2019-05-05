/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb.javadsl

import akka.NotUsed
import akka.stream.alpakka.influxdb.{InfluxDBSettings, InfluxDBWriteMessage, InfluxDBWriteResult}
import org.influxdb.InfluxDB

object InfluxDBFlow {

  def create[T](
      settings: InfluxDBSettings,
      influxDB: InfluxDB
  ): akka.stream.javadsl.Flow[InfluxDBWriteMessage[T, NotUsed], InfluxDBWriteResult[T, NotUsed], NotUsed] =
    akka.stream.alpakka.influxdb.scaladsl.InfluxDBFlow.create(settings)(influxDB).asJava

  def typed[T](
      clazz: Class[T],
      settings: InfluxDBSettings,
      influxDB: InfluxDB
  ): akka.stream.javadsl.Flow[InfluxDBWriteMessage[T, NotUsed], InfluxDBWriteResult[T, NotUsed], NotUsed] =
    akka.stream.alpakka.influxdb.scaladsl.InfluxDBFlow.typed(clazz, settings)(influxDB).asJava

  def createWithPassThrough[T, C](
      settings: InfluxDBSettings,
      influxDB: InfluxDB
  ): akka.stream.javadsl.Flow[InfluxDBWriteMessage[T, C], InfluxDBWriteResult[T, C], NotUsed] =
    akka.stream.alpakka.influxdb.scaladsl.InfluxDBFlow.createWithPassThrough(settings)(influxDB).asJava

  def typedWithPassThrough[T, C](
      clazz: Class[T],
      settings: InfluxDBSettings,
      influxDB: InfluxDB
  ): akka.stream.javadsl.Flow[InfluxDBWriteMessage[T, C], InfluxDBWriteResult[T, C], NotUsed] =
    akka.stream.alpakka.influxdb.scaladsl.InfluxDBFlow.typedWithPassThrough(clazz, settings)(influxDB).asJava

  def createWithContext[T, C](
      settings: InfluxDBSettings,
      influxDB: InfluxDB
  ): akka.stream.javadsl.Flow[(InfluxDBWriteMessage[T, NotUsed], C), (InfluxDBWriteResult[T, C], C), NotUsed] =
    akka.stream.alpakka.influxdb.scaladsl.InfluxDBFlow.createWithContext(settings)(influxDB).asJava

  def typedWithContext[T, C](
      clazz: Class[T],
      settings: InfluxDBSettings,
      influxDB: InfluxDB
  ): akka.stream.javadsl.Flow[(InfluxDBWriteMessage[T, NotUsed], C), (InfluxDBWriteResult[T, C], C), NotUsed] =
    akka.stream.alpakka.influxdb.scaladsl.InfluxDBFlow.typedWithContext(clazz, settings)(influxDB).asJava

}
