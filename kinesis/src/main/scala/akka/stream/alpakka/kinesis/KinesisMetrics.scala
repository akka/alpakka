/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

object KinesisMetrics extends Enumeration {
  val NoTag = Map.empty[String, String]

  case class Counter(name: String)
  case class Gauge(name: String)
  case class Record(name: String)

  val AggRecordSize = Record("agg-record-size")
  val AggRecordSuccess = Counter("agg-record-success")
  val AggRecordFailure = Counter("agg-record-failure")
  val AggRecordDropped = Counter("agg-record-dropped")
  val AggRecordRetryable = Counter("agg-record-retryable")
  val AggregationGroups = Gauge("aggregation-groups")
  val BufferedPayloadBytes = Gauge("buffered-payload-bytes")
  val UserRecordSize = Record("user-record-size")
  val UserRecordSuccess = Counter("user-record-success")
  val UserRecordFailure = Counter("user-record-failure")
  val UserRecordDropped = Counter("user-record-dropped")
  val UserRecordTooLarge = Counter("user-record-too-large")
  val BytesSuccess = Counter("bytes-success")
  val BytesFailure = Counter("bytes-failure")
  val BytesDropped = Counter("bytes-dropped")
  val BytesRetryable = Counter("bytes-retryable")
  val BytesSent = Counter("bytes-sent")
  val BatchRequestBytes = Record("batch-request-bytes")
}

trait KinesisMetrics {
  import KinesisMetrics._
  def count(metric: Counter, value: Long, tags: Map[String, String] = NoTag): Unit
  def gauge(metric: Gauge, value: Long, tags: Map[String, String] = NoTag): Unit
  def record(metric: Record, value: Long, tags: Map[String, String] = NoTag): Unit

  def increment(metric: Counter, tags: Map[String, String] = NoTag): Unit = {
    count(metric, 1, tags)
  }
}
