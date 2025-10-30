/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.influxdb

import java.util.concurrent.TimeUnit

import akka.annotation.ApiMayChange

/**
 * API may change.
 */
@ApiMayChange
object InfluxDbReadSettings {
  val Default = new InfluxDbReadSettings(TimeUnit.MILLISECONDS)

  def apply(): InfluxDbReadSettings = Default

}

/**
 * API may change.
 */
@ApiMayChange
final class InfluxDbReadSettings private (val precision: TimeUnit) {

  def withPrecision(precision: TimeUnit): InfluxDbReadSettings = copy(precision = precision)

  private def copy(
      precision: TimeUnit
  ): InfluxDbReadSettings = new InfluxDbReadSettings(
    precision = precision
  )

  override def toString: String =
    s"""InfluxDbReadSettings(precision=$precision)"""

}
