package akka.stream.alpakka.influxdb

abstract class InfluxDBClientSettings {
  val host: String
  val port: Int
  val parallelism: Int
}
