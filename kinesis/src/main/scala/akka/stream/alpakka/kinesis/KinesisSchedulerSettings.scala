/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import scala.concurrent.duration._

final class KinesisSchedulerSourceSettings private (val bufferSize: Int, val backpressureTimeout: FiniteDuration) {
  require(
    bufferSize >= 1,
    "Buffer size must be greater than 0; use size 1 to disable stage buffering"
  )
  def withBufferSize(size: Int): KinesisSchedulerSourceSettings =
    KinesisSchedulerSourceSettings(size, backpressureTimeout)
  def withBackpressureTimeout(timeout: java.time.Duration): KinesisSchedulerSourceSettings =
    KinesisSchedulerSourceSettings(bufferSize, FiniteDuration.apply(timeout.toMillis, MILLISECONDS))
}
final class KinesisSchedulerCheckpointSettings private (val maxBatchSize: Int, val maxBatchWait: FiniteDuration) {
  require(
    maxBatchSize >= 1,
    "Batch size must be greater than 0; use size 1 to commit records one at a time"
  )

  def withMaxBatchSize(size: Int): KinesisSchedulerCheckpointSettings =
    KinesisSchedulerCheckpointSettings(size, maxBatchWait)
  def withMaxBatchWait(duration: java.time.Duration): KinesisSchedulerCheckpointSettings =
    KinesisSchedulerCheckpointSettings(maxBatchSize, FiniteDuration.apply(duration.toMillis, MILLISECONDS))
}

object KinesisSchedulerSourceSettings {

  def apply(bufferSize: Int, backpressureTimeout: FiniteDuration): KinesisSchedulerSourceSettings =
    new KinesisSchedulerSourceSettings(bufferSize, backpressureTimeout)
  def apply: KinesisSchedulerSourceSettings = defaults

  val defaults: KinesisSchedulerSourceSettings = KinesisSchedulerSourceSettings(1000, 1.minute)

  /**
   * Java API
   */
  def create(bufferSize: Int, backpressureTimeout: java.time.Duration): KinesisSchedulerSourceSettings =
    apply(bufferSize, FiniteDuration.apply(backpressureTimeout.toMillis, MILLISECONDS))

}

object KinesisSchedulerCheckpointSettings {

  def apply(maxBatchSize: Int, maxBatchWait: FiniteDuration): KinesisSchedulerCheckpointSettings =
    new KinesisSchedulerCheckpointSettings(maxBatchSize, maxBatchWait)
  def apply: KinesisSchedulerCheckpointSettings = defaults

  val defaults: KinesisSchedulerCheckpointSettings = KinesisSchedulerCheckpointSettings(1000, 10.seconds)

  /**
   * Java API
   */
  def create(maxBatchSize: Int, maxBatchWait: java.time.Duration): KinesisSchedulerCheckpointSettings =
    apply(maxBatchSize, FiniteDuration.apply(maxBatchWait.toMillis, MILLISECONDS))

}
