/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb.javadsl

import java.util.concurrent.CompletionStage

import akka.annotation.ApiMayChange
import akka.{Done, NotUsed}
import akka.stream.alpakka.influxdb.{InfluxDbWriteMessage, InfluxDbWriteResult}
import akka.stream.javadsl.{Keep, Sink}
import org.influxdb.InfluxDB
import org.influxdb.dto.Point

/**
 * Java API.
 */
@ApiMayChange
object InfluxDbSink {

  def create(
      influxDB: InfluxDB
  ): akka.stream.javadsl.Sink[java.util.List[InfluxDbWriteMessage[Point, NotUsed]], CompletionStage[Done]] =
    InfluxDbFlow
      .create(influxDB)
      .toMat(Sink.ignore[java.util.List[InfluxDbWriteResult[Point, NotUsed]]],
             Keep.right[NotUsed, CompletionStage[Done]])

  def typed[T](
      clazz: Class[T],
      influxDB: InfluxDB
  ): akka.stream.javadsl.Sink[java.util.List[InfluxDbWriteMessage[T, NotUsed]], CompletionStage[Done]] =
    InfluxDbFlow
      .typed(clazz, influxDB)
      .toMat(Sink.ignore[java.util.List[InfluxDbWriteResult[T, NotUsed]]], Keep.right[NotUsed, CompletionStage[Done]])

}
