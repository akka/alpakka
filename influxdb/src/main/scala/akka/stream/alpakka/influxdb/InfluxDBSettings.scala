package akka.stream.alpakka.influxdb

import org.influxdb.InfluxDBFactory

final class InfluxDBSettings private (
val host: String,
val port: Int,
val parallelism: Int,
) extends InfluxDBClientSettings {

  require(host.nonEmpty, "A host name must be provided.")
  require(port > -1, "A port number must be provided.")

  def withHost(value: String): InfluxDBSettings = copy(host = value)
  def withPort(value: Int): InfluxDBSettings = copy(port = value)
  def withParallelism(value: Int): InfluxDBSettings = copy(parallelism = value)

  private def copy(
      host: String = host,
      port: Int = port,
      parallelism: Int = parallelism
  ): InfluxDBSettings = new InfluxDBSettings(
    host = host,
    port = port,
    parallelism = parallelism,
  )


}
