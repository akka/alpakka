/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesisfirehose

import akka.stream.alpakka.kinesisfirehose.KinesisFirehoseFlowSettings.{Exponential, Linear}
import akka.util.JavaDurationConverters._

import scala.concurrent.duration._

final class KinesisFirehoseFlowSettings private (val parallelism: Int,
                                                 val maxBatchSize: Int,
                                                 val maxRecordsPerSecond: Int,
                                                 val maxBytesPerSecond: Int,
                                                 val maxRetries: Int,
                                                 val backoffStrategy: KinesisFirehoseFlowSettings.RetryBackoffStrategy,
                                                 val retryInitialTimeout: scala.concurrent.duration.FiniteDuration) {

  require(
    maxBatchSize >= 1 && maxBatchSize <= 500,
    "Limit must be between 1 and 500. See: https://docs.aws.amazon.com/firehose/latest/APIReference/API_PutRecordBatch.html"
  )
  require(maxRecordsPerSecond >= 1)
  require(maxBytesPerSecond >= 1)
  require(maxRetries >= 0)

  def withParallelism(value: Int): KinesisFirehoseFlowSettings = copy(parallelism = value)
  def withMaxBatchSize(value: Int): KinesisFirehoseFlowSettings = copy(maxBatchSize = value)
  def withMaxRecordsPerSecond(value: Int): KinesisFirehoseFlowSettings = copy(maxRecordsPerSecond = value)
  def withMaxBytesPerSecond(value: Int): KinesisFirehoseFlowSettings = copy(maxBytesPerSecond = value)
  def withMaxRetries(value: Int): KinesisFirehoseFlowSettings = copy(maxRetries = value)
  def withBackoffStrategyExponential(): KinesisFirehoseFlowSettings = copy(backoffStrategy = Exponential)
  def withBackoffStrategyLinear(): KinesisFirehoseFlowSettings = copy(backoffStrategy = Linear)
  def withBackoffStrategy(value: KinesisFirehoseFlowSettings.RetryBackoffStrategy): KinesisFirehoseFlowSettings =
    copy(backoffStrategy = value)

  /** Scala API */
  def withRetryInitialTimeout(value: scala.concurrent.duration.FiniteDuration): KinesisFirehoseFlowSettings =
    copy(retryInitialTimeout = value)

  /** Java API */
  def withRetryInitialTimeout(value: java.time.Duration): KinesisFirehoseFlowSettings =
    copy(retryInitialTimeout = value.asScala)

  private def copy(
      parallelism: Int = parallelism,
      maxBatchSize: Int = maxBatchSize,
      maxRecordsPerSecond: Int = maxRecordsPerSecond,
      maxBytesPerSecond: Int = maxBytesPerSecond,
      maxRetries: Int = maxRetries,
      backoffStrategy: KinesisFirehoseFlowSettings.RetryBackoffStrategy = backoffStrategy,
      retryInitialTimeout: scala.concurrent.duration.FiniteDuration = retryInitialTimeout
  ): KinesisFirehoseFlowSettings = new KinesisFirehoseFlowSettings(
    parallelism = parallelism,
    maxBatchSize = maxBatchSize,
    maxRecordsPerSecond = maxRecordsPerSecond,
    maxBytesPerSecond = maxBytesPerSecond,
    maxRetries = maxRetries,
    backoffStrategy = backoffStrategy,
    retryInitialTimeout = retryInitialTimeout
  )

  override def toString =
    "KinesisFirehoseFlowSettings(" +
    s"parallelism=$parallelism," +
    s"maxBatchSize=$maxBatchSize," +
    s"maxRecordsPerSecond=$maxRecordsPerSecond," +
    s"maxBytesPerSecond=$maxBytesPerSecond," +
    s"maxRetries=$maxRetries," +
    s"backoffStrategy=$backoffStrategy," +
    s"retryInitialTimeout=${retryInitialTimeout.toCoarsest}" +
    ")"
}

object KinesisFirehoseFlowSettings {
  private val MaxRecordsPerRequest = 500
  private val MaxRecordsPerSecond = 5000
  private val MaxBytesPerSecond = 4000000

  sealed trait RetryBackoffStrategy
  case object Exponential extends RetryBackoffStrategy
  case object Linear extends RetryBackoffStrategy

  val exponential: RetryBackoffStrategy = Exponential
  val linear: RetryBackoffStrategy = Linear

  val Defaults: KinesisFirehoseFlowSettings = new KinesisFirehoseFlowSettings(
    parallelism = MaxRecordsPerSecond / MaxRecordsPerRequest,
    maxBatchSize = MaxRecordsPerRequest,
    maxRecordsPerSecond = MaxRecordsPerSecond,
    maxBytesPerSecond = MaxBytesPerSecond,
    maxRetries = 5,
    backoffStrategy = Exponential,
    retryInitialTimeout = 100.millis
  )

  /** Scala API */
  def apply(): KinesisFirehoseFlowSettings = Defaults

  /** Java API */
  def create(): KinesisFirehoseFlowSettings = Defaults
}
