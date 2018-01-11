/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import KinesisFlowSettings._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.language.postfixOps

case class KinesisFlowSettings(parallelism: Int,
                               maxBatchSize: Int,
                               maxRecordsPerSecond: Int,
                               maxBytesPerSecond: Int,
                               maxRetries: Int,
                               backoffStrategy: RetryBackoffStrategy,
                               retryInitialTimeout: FiniteDuration) {
  require(
    maxBatchSize >= 1 && maxBatchSize <= 500,
    "Limit must be between 1 and 500. See: http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html"
  )
  require(maxRecordsPerSecond >= 1)
  require(maxBytesPerSecond >= 1)
  require(maxRetries >= 0)
}

object KinesisFlowSettings {

  private val MAX_RECORDS_PER_REQUEST = 500
  private val MAX_RECORDS_PER_SHARD_PER_SECOND = 1000
  private val MAX_BYTES_PER_SHARD_PER_SECOND = 1000000

  sealed trait RetryBackoffStrategy
  case object Exponential extends RetryBackoffStrategy
  case object Lineal extends RetryBackoffStrategy

  val exponential = Exponential
  val lineal = Lineal

  val defaultInstance = byNumberOfShards(1)

  def byNumberOfShards(shards: Int): KinesisFlowSettings =
    KinesisFlowSettings(
      parallelism = shards * (MAX_RECORDS_PER_SHARD_PER_SECOND / MAX_RECORDS_PER_REQUEST),
      maxBatchSize = MAX_RECORDS_PER_REQUEST,
      maxRecordsPerSecond = shards * MAX_RECORDS_PER_SHARD_PER_SECOND,
      maxBytesPerSecond = shards * MAX_BYTES_PER_SHARD_PER_SECOND,
      maxRetries = 5,
      backoffStrategy = Exponential,
      retryInitialTimeout = 100 millis
    )
}
