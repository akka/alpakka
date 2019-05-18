/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb.scaladsl

import akka.{Done, NotUsed}
import akka.stream.alpakka.influxdb.{InfluxDbSettings, InfluxDbWriteMessage}
import akka.stream.scaladsl.{Keep, Sink}
import org.influxdb.InfluxDB
import org.influxdb.dto.Point

import scala.concurrent.Future

object InfluxDbSink {

  def apply(
      settings: InfluxDbSettings
  )(implicit influxDB: InfluxDB): Sink[InfluxDbWriteMessage[Point, NotUsed], Future[Done]] =
    InfluxDbFlow.create[Point](settings).toMat(Sink.ignore)(Keep.right)

  def typed[T](
      clazz: Class[T],
      settings: InfluxDbSettings
  )(implicit influxDB: InfluxDB): Sink[InfluxDbWriteMessage[T, NotUsed], Future[Done]] =
    InfluxDbFlow
      .typed(clazz, settings)
      .toMat(Sink.ignore)(Keep.right)

}
