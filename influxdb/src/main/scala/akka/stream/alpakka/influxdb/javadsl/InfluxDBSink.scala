package akka.stream.alpakka.influxdb.javadsl

import java.util.concurrent.CompletionStage

import akka.{Done, NotUsed}
import akka.stream.alpakka.influxdb.{InfluxDBSettings, InfluxDBWriteMessage, InfluxDBWriteResult}
import akka.stream.javadsl.{Keep, Sink}
import org.influxdb.InfluxDB
import org.influxdb.dto.Point

/**
 * Java API.
 */
object InfluxDBSink {

  def create(
      settings: InfluxDBSettings,
      influxDB: InfluxDB
  ): akka.stream.javadsl.Sink[InfluxDBWriteMessage[Point, NotUsed], CompletionStage[Done]] =
    InfluxDBFlow
      .create(settings, influxDB)
      .toMat(Sink.ignore[InfluxDBWriteResult[Point, NotUsed]], Keep.right[NotUsed, CompletionStage[Done]])

  def typed[T](clazz: Class[T],
               settings: InfluxDBSettings,
               influxDB: InfluxDB): akka.stream.javadsl.Sink[InfluxDBWriteMessage[T, NotUsed], CompletionStage[Done]] =
    InfluxDBFlow
      .typed(clazz, settings, influxDB)
      .toMat(Sink.ignore[InfluxDBWriteResult[T, NotUsed]], Keep.right[NotUsed, CompletionStage[Done]])

}
