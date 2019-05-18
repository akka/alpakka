/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.TimeUnit

object InfluxDbSettings {
  val Default = new InfluxDbSettings(batchSize = 10, TimeUnit.MILLISECONDS)

  def apply(): InfluxDbSettings = Default

}

final class InfluxDbSettings private (
    val batchSize: Int,
    val precision: TimeUnit
) {

  def withBatchSize(value: Int): InfluxDbSettings = copy(batchSize = value)

  private def copy(
      batchSize: Int = batchSize,
      precision: TimeUnit = precision
  ): InfluxDbSettings = new InfluxDbSettings(
    batchSize = batchSize,
    precision = precision
  )

}
