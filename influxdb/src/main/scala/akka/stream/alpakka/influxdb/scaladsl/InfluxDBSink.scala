package akka.stream.alpakka.influxdb.scaladsl

import akka.{Done, NotUsed}
import akka.stream.alpakka.influxdb.{InfluxDBSettings, InfluxDBWriteMessage}
import akka.stream.scaladsl.{Keep, Sink}
import org.influxdb.InfluxDB
import org.influxdb.dto.Point

import scala.concurrent.Future

object InfluxDBSink {

  def apply(
      settings: InfluxDBSettings
  )(implicit influxDB: InfluxDB): Sink[InfluxDBWriteMessage[Point, NotUsed], Future[Done]] =
    InfluxDBFlow.create[Point](settings).toMat(Sink.ignore)(Keep.right)

  def typed[T](
      clazz: Class[T],
      settings: InfluxDBSettings
  )(implicit influxDB: InfluxDB): Sink[InfluxDBWriteMessage[T, NotUsed], Future[Done]] =
    InfluxDBFlow
      .typed(clazz, settings)
      .toMat(Sink.ignore)(Keep.right)

}
