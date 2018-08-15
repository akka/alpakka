/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.{FiniteDuration, TimeUnit, _}
import akka.util.JavaDurationConverters._

final class SqsBatchFlowSettings private (val maxBatchSize: Int,
                                          val maxBatchWait: scala.concurrent.duration.FiniteDuration,
                                          val concurrentRequests: Int) {

  require(
    maxBatchSize > 0 && maxBatchSize <= 10,
    s"Invalid value for maxBatchSize: $maxBatchSize. It should be 0 < maxBatchSize < 10, due to the Amazon SQS requirements."
  )

  def withMaxBatchSize(value: Int): SqsBatchFlowSettings = copy(maxBatchSize = value)

  /** Scala API */
  def withMaxBatchWait(value: scala.concurrent.duration.FiniteDuration): SqsBatchFlowSettings =
    copy(maxBatchWait = value)

  /** Java API */
  def withMaxBatchWait(value: java.time.Duration): SqsBatchFlowSettings =
    withMaxBatchWait(
      scala.concurrent.duration.FiniteDuration(value.toMillis, java.util.concurrent.TimeUnit.MILLISECONDS)
    )

  /**
   * Java API
   *
   * @deprecated use withMaxBatchWait(java.time.Duration) instead
   */
  @deprecated("use withMaxBatchWait(java.time.Duration) instead", "0.21")
  def withMaxBatchWait(length: Long, unit: TimeUnit): SqsBatchFlowSettings =
    copy(maxBatchWait = FiniteDuration(length, unit))

  def withConcurrentRequests(value: Int): SqsBatchFlowSettings = copy(concurrentRequests = value)

  private def copy(maxBatchSize: Int = maxBatchSize,
                   maxBatchWait: scala.concurrent.duration.FiniteDuration = maxBatchWait,
                   concurrentRequests: Int = concurrentRequests): SqsBatchFlowSettings =
    new SqsBatchFlowSettings(maxBatchSize = maxBatchSize,
                             maxBatchWait = maxBatchWait,
                             concurrentRequests = concurrentRequests)

  override def toString =
    s"""SqsBatchFlowSettings(maxBatchSize=$maxBatchSize,maxBatchWait=$maxBatchWait,concurrentRequests=$concurrentRequests)"""

}

object SqsBatchFlowSettings {

  val Defaults = new SqsBatchFlowSettings(
    maxBatchSize = 10,
    maxBatchWait = 500.millis,
    concurrentRequests = 1
  )

  /** Scala API */
  def apply(): SqsBatchFlowSettings = Defaults

  /** Java API */
  def getInstance(): SqsBatchFlowSettings = apply()

  /** Scala API */
  def apply(
      maxBatchSize: Int,
      maxBatchWait: scala.concurrent.duration.FiniteDuration,
      concurrentRequests: Int
  ): SqsBatchFlowSettings = new SqsBatchFlowSettings(
    maxBatchSize,
    maxBatchWait,
    concurrentRequests
  )

  /** Java API */
  def create(
      maxBatchSize: Int,
      maxBatchWait: java.time.Duration,
      concurrentRequests: Int
  ): SqsBatchFlowSettings = new SqsBatchFlowSettings(
    maxBatchSize,
    maxBatchWait.asScala,
    concurrentRequests
  )
}
