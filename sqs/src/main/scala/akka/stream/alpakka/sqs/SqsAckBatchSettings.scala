/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs
import scala.concurrent.duration.{FiniteDuration, TimeUnit}
import scala.concurrent.duration._
import akka.util.JavaDurationConverters._

final class SqsAckBatchSettings private (val maxBatchSize: Int,
                                         val maxBatchWait: scala.concurrent.duration.FiniteDuration,
                                         val concurrentRequests: Int) {

  require(concurrentRequests > 0)
  require(
    maxBatchSize > 0 && maxBatchSize <= 10,
    s"Invalid value for maxBatchSize: $maxBatchSize. It should be 0 < maxBatchSize < 10, due to the Amazon SQS requirements."
  )

  def withMaxBatchSize(value: Int): SqsAckBatchSettings = copy(maxBatchSize = value)

  /** Scala API */
  def withMaxBatchWait(value: scala.concurrent.duration.FiniteDuration): SqsAckBatchSettings =
    copy(maxBatchWait = value)

  /** Java API */
  def withMaxBatchWait(value: java.time.Duration): SqsAckBatchSettings =
    withMaxBatchWait(
      scala.concurrent.duration.FiniteDuration(value.toMillis, java.util.concurrent.TimeUnit.MILLISECONDS)
    )

  /**
   * Java API
   *
   * @deprecated use withMaxBatchWait(java.time.Duration) instead
   */
  @deprecated("use withMaxBatchWait(java.time.Duration) instead", "0.21")
  def withMaxBatchWait(length: Long, unit: TimeUnit): SqsAckBatchSettings =
    this.copy(maxBatchWait = FiniteDuration(length, unit))

  def withConcurrentRequests(value: Int): SqsAckBatchSettings = copy(concurrentRequests = value)

  private def copy(maxBatchSize: Int = maxBatchSize,
                   maxBatchWait: scala.concurrent.duration.FiniteDuration = maxBatchWait,
                   concurrentRequests: Int = concurrentRequests): SqsAckBatchSettings =
    new SqsAckBatchSettings(maxBatchSize = maxBatchSize,
                            maxBatchWait = maxBatchWait,
                            concurrentRequests = concurrentRequests)

  override def toString =
    s"""SqsAckBatchSettings(maxBatchSize=$maxBatchSize,maxBatchWait=$maxBatchWait,concurrentRequests=$concurrentRequests)"""

}
object SqsAckBatchSettings {
  val Defaults = SqsAckBatchSettings(
    maxBatchSize = 10,
    maxBatchWait = 500.millis,
    concurrentRequests = 1
  )

  /** Scala API */
  def apply(): SqsAckBatchSettings = Defaults

  /** Java API */
  def create(): SqsAckBatchSettings = Defaults

  /** Scala API */
  def apply(
      maxBatchSize: Int,
      maxBatchWait: scala.concurrent.duration.FiniteDuration,
      concurrentRequests: Int
  ): SqsAckBatchSettings = new SqsAckBatchSettings(
    maxBatchSize,
    maxBatchWait,
    concurrentRequests
  )

  /** Java API */
  def create(
      maxBatchSize: Int,
      maxBatchWait: java.time.Duration,
      concurrentRequests: Int
  ): SqsAckBatchSettings = new SqsAckBatchSettings(
    maxBatchSize,
    maxBatchWait.asScala,
    concurrentRequests
  )
}
