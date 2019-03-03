/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import akka.stream.alpakka.kinesis.KinesisFlowSettings.{Exponential, Linear}
import akka.util.JavaDurationConverters._

import scala.concurrent.duration._

final class KinesisFlowSettings private (val parallelism: Int,
                                         val maxBatchSize: Int,
                                         val maxRecordsPerSecond: Int,
                                         val maxBytesPerSecond: Int,
                                         val maxRetries: Int,
                                         val backoffStrategy: KinesisFlowSettings.RetryBackoffStrategy,
                                         val retryInitialTimeout: scala.concurrent.duration.FiniteDuration) {

  require(
    maxBatchSize >= 1 && maxBatchSize <= 500,
    "Limit must be between 1 and 500. See: http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html"
  )
  require(maxRecordsPerSecond >= 1)
  require(maxBytesPerSecond >= 1)
  require(maxRetries >= 0)

  def withParallelism(value: Int): KinesisFlowSettings = copy(parallelism = value)
  def withMaxBatchSize(value: Int): KinesisFlowSettings = copy(maxBatchSize = value)
  def withMaxRecordsPerSecond(value: Int): KinesisFlowSettings = copy(maxRecordsPerSecond = value)
  def withMaxBytesPerSecond(value: Int): KinesisFlowSettings = copy(maxBytesPerSecond = value)
  def withMaxRetries(value: Int): KinesisFlowSettings = copy(maxRetries = value)
  def withBackoffStrategyExponential(): KinesisFlowSettings = copy(backoffStrategy = Exponential)
  def withBackoffStrategyLinear(): KinesisFlowSettings = copy(backoffStrategy = Linear)
  def withBackoffStrategy(value: KinesisFlowSettings.RetryBackoffStrategy): KinesisFlowSettings =
    copy(backoffStrategy = value)

  /** Scala API */
  def withRetryInitialTimeout(value: scala.concurrent.duration.FiniteDuration): KinesisFlowSettings =
    copy(retryInitialTimeout = value)

  /** Java API */
  def withRetryInitialTimeout(value: java.time.Duration): KinesisFlowSettings =
    copy(retryInitialTimeout = value.asScala)

  private def copy(
      parallelism: Int = parallelism,
      maxBatchSize: Int = maxBatchSize,
      maxRecordsPerSecond: Int = maxRecordsPerSecond,
      maxBytesPerSecond: Int = maxBytesPerSecond,
      maxRetries: Int = maxRetries,
      backoffStrategy: KinesisFlowSettings.RetryBackoffStrategy = backoffStrategy,
      retryInitialTimeout: scala.concurrent.duration.FiniteDuration = retryInitialTimeout
  ): KinesisFlowSettings = new KinesisFlowSettings(
    parallelism = parallelism,
    maxBatchSize = maxBatchSize,
    maxRecordsPerSecond = maxRecordsPerSecond,
    maxBytesPerSecond = maxBytesPerSecond,
    maxRetries = maxRetries,
    backoffStrategy = backoffStrategy,
    retryInitialTimeout = retryInitialTimeout
  )

  override def toString =
    "KinesisFlowSettings(" +
    s"parallelism=$parallelism," +
    s"maxBatchSize=$maxBatchSize," +
    s"maxRecordsPerSecond=$maxRecordsPerSecond," +
    s"maxBytesPerSecond=$maxBytesPerSecond," +
    s"maxRetries=$maxRetries," +
    s"backoffStrategy=$backoffStrategy," +
    s"retryInitialTimeout=${retryInitialTimeout.toCoarsest}" +
    ")"
}

object KinesisFlowSettings {
  private val MAX_RECORDS_PER_REQUEST = 500
  private val MAX_RECORDS_PER_SHARD_PER_SECOND = 1000
  private val MAX_BYTES_PER_SHARD_PER_SECOND = 1000000

  sealed trait RetryBackoffStrategy
  case object Exponential extends RetryBackoffStrategy
  case object Linear extends RetryBackoffStrategy

  val Defaults: KinesisFlowSettings = byNumberOfShards(1)

  def apply(): KinesisFlowSettings = Defaults

  def byNumberOfShards(shards: Int): KinesisFlowSettings =
    new KinesisFlowSettings(
      parallelism = shards * (MAX_RECORDS_PER_SHARD_PER_SECOND / MAX_RECORDS_PER_REQUEST),
      maxBatchSize = MAX_RECORDS_PER_REQUEST,
      maxRecordsPerSecond = shards * MAX_RECORDS_PER_SHARD_PER_SECOND,
      maxBytesPerSecond = shards * MAX_BYTES_PER_SHARD_PER_SECOND,
      maxRetries = 5,
      backoffStrategy = Exponential,
      retryInitialTimeout = 100.millis
    )

  /** Java API */
  def create(): KinesisFlowSettings = Defaults

  /** Java API */
  val exponential: RetryBackoffStrategy = Exponential

  /** Java API */
  val linear: RetryBackoffStrategy = Linear

}
