/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb.javadsl

import akka.NotUsed
import akka.annotation.ApiMayChange
import akka.stream.alpakka.influxdb.{InfluxDbWriteMessage, InfluxDbWriteResult}
import org.influxdb.InfluxDB
import akka.stream.javadsl.Flow
import akka.stream.alpakka.influxdb.scaladsl
import org.influxdb.dto.Point

import scala.jdk.CollectionConverters._

/**
 * API may change.
 */
@ApiMayChange
object InfluxDbFlow {

  def create(
      influxDB: InfluxDB
  ): Flow[java.util.List[InfluxDbWriteMessage[Point, NotUsed]],
          java.util.List[InfluxDbWriteResult[Point, NotUsed]],
          NotUsed] =
    akka.stream.scaladsl
      .Flow[java.util.List[InfluxDbWriteMessage[Point, NotUsed]]]
      .map(_.asScala.toList)
      .via(scaladsl.InfluxDbFlow.create()(influxDB))
      .map(_.asJava)
      .asJava

  def typed[T](
      clazz: Class[T],
      influxDB: InfluxDB
  ): Flow[java.util.List[InfluxDbWriteMessage[T, NotUsed]], java.util.List[InfluxDbWriteResult[T, NotUsed]], NotUsed] =
    akka.stream.scaladsl
      .Flow[java.util.List[InfluxDbWriteMessage[T, NotUsed]]]
      .map(_.asScala.toList)
      .via(scaladsl.InfluxDbFlow.typed(clazz)(influxDB))
      .map(_.asJava)
      .asJava

  def createWithPassThrough[C](
      influxDB: InfluxDB
  ): Flow[java.util.List[InfluxDbWriteMessage[Point, C]], java.util.List[InfluxDbWriteResult[Point, C]], NotUsed] =
    akka.stream.scaladsl
      .Flow[java.util.List[InfluxDbWriteMessage[Point, C]]]
      .map(_.asScala.toList)
      .via(scaladsl.InfluxDbFlow.createWithPassThrough(influxDB))
      .map(_.asJava)
      .asJava

  def typedWithPassThrough[T, C](
      clazz: Class[T],
      influxDB: InfluxDB
  ): Flow[java.util.List[InfluxDbWriteMessage[T, C]], java.util.List[InfluxDbWriteResult[T, C]], NotUsed] =
    akka.stream.scaladsl
      .Flow[java.util.List[InfluxDbWriteMessage[T, C]]]
      .map(_.asScala.toList)
      .via(scaladsl.InfluxDbFlow.typedWithPassThrough(clazz)(influxDB))
      .map(_.asJava)
      .asJava

}
