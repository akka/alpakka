/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

final class SqsPublishGroupedSettings private (val maxBatchSize: Int,
                                               val maxBatchWait: scala.concurrent.duration.FiniteDuration,
                                               val concurrentRequests: Int) {

  require(
    maxBatchSize > 0 && maxBatchSize <= 10,
    s"Invalid value for maxBatchSize: $maxBatchSize. It should be 0 < maxBatchSize < 10, due to the Amazon SQS requirements."
  )

  def withMaxBatchSize(value: Int): SqsPublishGroupedSettings = copy(maxBatchSize = value)

  /** Scala API */
  def withMaxBatchWait(value: scala.concurrent.duration.FiniteDuration): SqsPublishGroupedSettings =
    copy(maxBatchWait = value)

  /** Java API */
  def withMaxBatchWait(value: java.time.Duration): SqsPublishGroupedSettings =
    withMaxBatchWait(
      scala.concurrent.duration.FiniteDuration(value.toMillis, java.util.concurrent.TimeUnit.MILLISECONDS)
    )

  /**
   * Java API
   *
   * @deprecated use withMaxBatchWait(java.time.Duration) instead
   */
  @deprecated("use withMaxBatchWait(java.time.Duration) instead", "1.0-M1")
  def withMaxBatchWait(length: Long, unit: TimeUnit): SqsPublishGroupedSettings =
    copy(maxBatchWait = FiniteDuration(length, unit))

  def withConcurrentRequests(value: Int): SqsPublishGroupedSettings = copy(concurrentRequests = value)

  private def copy(maxBatchSize: Int = maxBatchSize,
                   maxBatchWait: scala.concurrent.duration.FiniteDuration = maxBatchWait,
                   concurrentRequests: Int = concurrentRequests): SqsPublishGroupedSettings =
    new SqsPublishGroupedSettings(maxBatchSize = maxBatchSize,
                                  maxBatchWait = maxBatchWait,
                                  concurrentRequests = concurrentRequests)

  override def toString =
    s"""SqsPublishGroupedSettings(maxBatchSize=$maxBatchSize,maxBatchWait=$maxBatchWait,concurrentRequests=$concurrentRequests)"""

}

object SqsPublishGroupedSettings {

  val Defaults = new SqsPublishGroupedSettings(
    maxBatchSize = 10,
    maxBatchWait = 500.millis,
    concurrentRequests = 1
  )

  /** Scala API */
  def apply(): SqsPublishGroupedSettings = Defaults

  /** Java API */
  def create(): SqsPublishGroupedSettings = Defaults
}
