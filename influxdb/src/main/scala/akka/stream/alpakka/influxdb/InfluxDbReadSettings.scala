/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb

import java.util.concurrent.TimeUnit

object InfluxDbReadSettings {
  val Default = new InfluxDbReadSettings(TimeUnit.MILLISECONDS)

  def apply(): InfluxDbReadSettings = Default

}

final class InfluxDbReadSettings private (val precision: TimeUnit) {

  def withPrecision(value: TimeUnit): InfluxDbReadSettings = copy(precision = value)

  private def copy(
      precision: TimeUnit = precision
  ): InfluxDbReadSettings = new InfluxDbReadSettings(
    precision = precision
  )

  override def toString: String =
    s"""InfluxDbReadSettings(precision=$precision)"""

}
