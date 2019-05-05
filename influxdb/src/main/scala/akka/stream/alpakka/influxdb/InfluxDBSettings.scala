package akka.stream.alpakka.influxdb

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.TimeUnit

object InfluxDBSettings {
  val Default = new InfluxDBSettings(batchSize= 10, TimeUnit.MILLISECONDS)

  def apply(): InfluxDBSettings = Default

}

final class InfluxDBSettings private(
val batchSize: Int, val precision: TimeUnit
) {

  def withBatchSize(value: Int): InfluxDBSettings = copy(batchSize = value)

  private def copy(
      batchSize: Int = batchSize,
      precision: TimeUnit = precision
  ): InfluxDBSettings = new InfluxDBSettings(
    batchSize = batchSize,
    precision = precision
  )

}
