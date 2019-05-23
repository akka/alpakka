/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.influxdb

object InfluxDbWriteSettings {
  val Default = new InfluxDbWriteSettings(batchSize = 10)

  def apply(): InfluxDbWriteSettings = Default

}

final class InfluxDbWriteSettings private (
    val batchSize: Int,
) {

  def withBatchSize(value: Int): InfluxDbWriteSettings = copy(batchSize = value)

  private def copy(
      batchSize: Int = batchSize,
  ): InfluxDbWriteSettings = new InfluxDbWriteSettings(
    batchSize = batchSize
  )

  override def toString: String =
    s"""InfluxDbWriteSettings(batchSize=$batchSize)"""

}
