/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb.javadsl

import java.util.concurrent.CompletionStage

import akka.{Done, NotUsed}
import akka.stream.alpakka.influxdb.{InfluxDbSettings, InfluxDbWriteMessage, InfluxDbWriteResult}
import akka.stream.javadsl.{Keep, Sink}
import org.influxdb.InfluxDB
import org.influxdb.dto.Point

/**
 * Java API.
 */
object InfluxDbSink {

  def create(
      settings: InfluxDbSettings,
      influxDB: InfluxDB
  ): akka.stream.javadsl.Sink[InfluxDbWriteMessage[Point, NotUsed], CompletionStage[Done]] =
    InfluxDbFlow
      .create(settings, influxDB)
      .toMat(Sink.ignore[InfluxDbWriteResult[Point, NotUsed]], Keep.right[NotUsed, CompletionStage[Done]])

  def typed[T](clazz: Class[T],
               settings: InfluxDbSettings,
               influxDB: InfluxDB): akka.stream.javadsl.Sink[InfluxDbWriteMessage[T, NotUsed], CompletionStage[Done]] =
    InfluxDbFlow
      .typed(clazz, settings, influxDB)
      .toMat(Sink.ignore[InfluxDbWriteResult[T, NotUsed]], Keep.right[NotUsed, CompletionStage[Done]])

}
