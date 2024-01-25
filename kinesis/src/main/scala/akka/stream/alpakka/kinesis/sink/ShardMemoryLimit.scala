/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.sink

import akka.stream.alpakka.kinesis.KinesisMetrics
import akka.stream.alpakka.kinesis.KinesisMetrics._

import java.util.concurrent.atomic.AtomicLong

/** Tracks the total number of payload bytes of all the records either in the
 * buffer or being aggregated in any instance of `AggRecord`
 */
class ShardMemoryLimit(maxPerShard: Long, maxTotal: Long, bufferedPayloadBytesTotal: AtomicLong) {
  private var bufferedPayloadBytes: Long = 0
  def get: Long = bufferedPayloadBytes

  def add(delta: Int): Unit = {
    bufferedPayloadBytes += delta
    bufferedPayloadBytesTotal.addAndGet(delta)
  }

  def sync(metrics: KinesisMetrics): Unit =
    metrics.gauge(BufferedPayloadBytes, bufferedPayloadBytesTotal.get())

  def shouldDropOrBackpressure: Boolean = {
    // Each shard is allowed to use more than its share of the buffer capacity
    // as long as the total limit is not exceeded, and when that happens only
    // shards that used more than average will drop data or backpressure.
    bufferedPayloadBytesTotal.get() > maxTotal && bufferedPayloadBytes > maxPerShard
  }
}
