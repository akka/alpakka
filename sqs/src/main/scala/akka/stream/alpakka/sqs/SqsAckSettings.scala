/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import scala.concurrent.duration._
import akka.util.JavaDurationConverters._

final class SqsAckSinkSettings private (val maxInFlight: Int) {

  require(maxInFlight > 0)

  def withMaxInFlight(value: Int): SqsAckSinkSettings = copy(maxInFlight = value)

  private def copy(maxInFlight: Int = maxInFlight): SqsAckSinkSettings =
    new SqsAckSinkSettings(maxInFlight = maxInFlight)

  override def toString =
    s"""SqsAckSinkSettings(maxInFlight=$maxInFlight)"""
}

object SqsAckSinkSettings {

  val Defaults = new SqsAckSinkSettings(maxInFlight = 10)

  /** Scala API */
  def apply(): SqsAckSinkSettings = Defaults

  /** Java API */
  def getInstance(): SqsAckSinkSettings = apply()

  /** Scala API */
  def apply(
      maxInFlight: Int
  ): SqsAckSinkSettings = new SqsAckSinkSettings(
    maxInFlight
  )

  /** Java API */
  def create(
      maxInFlight: Int
  ): SqsAckSinkSettings = new SqsAckSinkSettings(
    maxInFlight
  )
}

final class SqsBatchAckFlowSettings private (val maxBatchSize: Int,
                                             val maxBatchWait: scala.concurrent.duration.FiniteDuration,
                                             val concurrentRequests: Int) {

  require(concurrentRequests > 0)
  require(
    maxBatchSize > 0 && maxBatchSize <= 10,
    s"Invalid value for maxBatchSize: $maxBatchSize. It should be 0 < maxBatchSize < 10, due to the Amazon SQS requirements."
  )

  def withMaxBatchSize(value: Int): SqsBatchAckFlowSettings = copy(maxBatchSize = value)

  /** Scala API */
  def withMaxBatchWait(value: scala.concurrent.duration.FiniteDuration): SqsBatchAckFlowSettings =
    copy(maxBatchWait = value)

  /** Java API */
  def withMaxBatchWait(value: java.time.Duration): SqsBatchAckFlowSettings =
    withMaxBatchWait(
      scala.concurrent.duration.FiniteDuration(value.toMillis, java.util.concurrent.TimeUnit.MILLISECONDS)
    )

  /**
   * Java API
   *
   * @deprecated use withMaxBatchWait(java.time.Duration) instead
   */
  @deprecated("use withMaxBatchWait(java.time.Duration) instead", "0.21")
  def withMaxBatchWait(length: Long, unit: TimeUnit): SqsBatchAckFlowSettings =
    this.copy(maxBatchWait = FiniteDuration(length, unit))

  def withConcurrentRequests(value: Int): SqsBatchAckFlowSettings = copy(concurrentRequests = value)

  private def copy(maxBatchSize: Int = maxBatchSize,
                   maxBatchWait: scala.concurrent.duration.FiniteDuration = maxBatchWait,
                   concurrentRequests: Int = concurrentRequests): SqsBatchAckFlowSettings =
    new SqsBatchAckFlowSettings(maxBatchSize = maxBatchSize,
                                maxBatchWait = maxBatchWait,
                                concurrentRequests = concurrentRequests)

  override def toString =
    s"""SqsBatchAckFlowSettings(maxBatchSize=$maxBatchSize,maxBatchWait=$maxBatchWait,concurrentRequests=$concurrentRequests)"""

}

object SqsBatchAckFlowSettings {
  val Defaults = SqsBatchAckFlowSettings(
    maxBatchSize = 10,
    maxBatchWait = 500.millis,
    concurrentRequests = 1
  )

  /** Scala API */
  def apply(): SqsBatchAckFlowSettings = Defaults

  /** Java API */
  def getInstance(): SqsBatchAckFlowSettings = apply()

  /** Scala API */
  def apply(
      maxBatchSize: Int,
      maxBatchWait: scala.concurrent.duration.FiniteDuration,
      concurrentRequests: Int
  ): SqsBatchAckFlowSettings = new SqsBatchAckFlowSettings(
    maxBatchSize,
    maxBatchWait,
    concurrentRequests
  )

  /** Java API */
  def create(
      maxBatchSize: Int,
      maxBatchWait: java.time.Duration,
      concurrentRequests: Int
  ): SqsBatchAckFlowSettings = new SqsBatchAckFlowSettings(
    maxBatchSize,
    maxBatchWait.asScala,
    concurrentRequests
  )
}
