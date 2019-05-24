/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb.scaladsl

import akka.{Done, NotUsed}
import akka.stream.alpakka.influxdb.{InfluxDbWriteMessage, InfluxDbWriteSettings}
import akka.stream.scaladsl.{Keep, Sink}
import org.influxdb.InfluxDB
import org.influxdb.dto.Point

import scala.concurrent.Future

object InfluxDbSink {

  def create(
      settings: InfluxDbWriteSettings
  )(implicit influxDB: InfluxDB): Sink[InfluxDbWriteMessage[Point, NotUsed], Future[Done]] =
    InfluxDbFlow.create(settings).toMat(Sink.ignore)(Keep.right)

  def typed[T](
      clazz: Class[T],
      settings: InfluxDbWriteSettings
  )(implicit influxDB: InfluxDB): Sink[InfluxDbWriteMessage[T, NotUsed], Future[Done]] =
    InfluxDbFlow
      .typed(clazz, settings)
      .toMat(Sink.ignore)(Keep.right)

}
