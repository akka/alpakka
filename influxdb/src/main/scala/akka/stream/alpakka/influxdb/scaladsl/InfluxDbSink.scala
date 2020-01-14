/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb.scaladsl

import akka.annotation.ApiMayChange
import akka.{Done, NotUsed}
import akka.stream.alpakka.influxdb.InfluxDbWriteMessage
import akka.stream.scaladsl.{Keep, Sink}
import org.influxdb.InfluxDB
import org.influxdb.dto.Point

import scala.concurrent.Future
import scala.collection.immutable

/**
 * API may change.
 */
@ApiMayChange
object InfluxDbSink {

  def create()(implicit influxDB: InfluxDB): Sink[immutable.Seq[InfluxDbWriteMessage[Point, NotUsed]], Future[Done]] =
    InfluxDbFlow.create.toMat(Sink.ignore)(Keep.right)

  def typed[T](
      clazz: Class[T]
  )(implicit influxDB: InfluxDB): Sink[immutable.Seq[InfluxDbWriteMessage[T, NotUsed]], Future[Done]] =
    InfluxDbFlow
      .typed(clazz)
      .toMat(Sink.ignore)(Keep.right)

}
