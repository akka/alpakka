/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesisfirehose

import akka.stream.alpakka.kinesisfirehose.KinesisFirehoseFlowSettings.{Exponential, Linear, RetryBackoffStrategy}

import scala.concurrent.duration._

case class KinesisFirehoseFlowSettings(parallelism: Int,
                                       maxBatchSize: Int,
                                       maxRecordsPerSecond: Int,
                                       maxBytesPerSecond: Int,
                                       maxRetries: Int,
                                       backoffStrategy: RetryBackoffStrategy,
                                       retryInitialTimeout: FiniteDuration) {
  require(
    maxBatchSize >= 1 && maxBatchSize <= 500,
    "Limit must be between 1 and 500. See: https://docs.aws.amazon.com/firehose/latest/APIReference/API_PutRecordBatch.html"
  )
  require(maxRecordsPerSecond >= 1)
  require(maxBytesPerSecond >= 1)
  require(maxRetries >= 0)

  def withParallelism(parallelism: Int): KinesisFirehoseFlowSettings = copy(parallelism = parallelism)

  def withMaxBatchSize(maxBatchSize: Int): KinesisFirehoseFlowSettings = copy(maxBatchSize = maxBatchSize)

  def withMaxRecordsPerSecond(maxRecordsPerSecond: Int): KinesisFirehoseFlowSettings =
    copy(maxRecordsPerSecond = maxRecordsPerSecond)

  def withMaxBytesPerSecond(maxBytesPerSecond: Int): KinesisFirehoseFlowSettings =
    copy(maxBytesPerSecond = maxBytesPerSecond)

  def withMaxRetries(maxRetries: Int): KinesisFirehoseFlowSettings = copy(maxRetries = maxRetries)

  def withBackoffStrategyExponential(): KinesisFirehoseFlowSettings = copy(backoffStrategy = Exponential)

  def withBackoffStrategyLinear(): KinesisFirehoseFlowSettings = copy(backoffStrategy = Linear)

  def withBackoffStrategy(backoffStrategy: RetryBackoffStrategy): KinesisFirehoseFlowSettings =
    copy(backoffStrategy = backoffStrategy)

  def withRetryInitialTimeout(timeout: Long, unit: java.util.concurrent.TimeUnit): KinesisFirehoseFlowSettings =
    copy(retryInitialTimeout = FiniteDuration(timeout, unit))
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

  val defaultInstance: KinesisFirehoseFlowSettings = KinesisFirehoseFlowSettings(
    parallelism = MaxRecordsPerSecond / MaxRecordsPerRequest,
    maxBatchSize = MaxRecordsPerRequest,
    maxRecordsPerSecond = MaxRecordsPerSecond,
    maxBytesPerSecond = MaxBytesPerSecond,
    maxRetries = 5,
    backoffStrategy = Exponential,
    retryInitialTimeout = 100.millis
  )

  def create(): KinesisFirehoseFlowSettings = defaultInstance
}
