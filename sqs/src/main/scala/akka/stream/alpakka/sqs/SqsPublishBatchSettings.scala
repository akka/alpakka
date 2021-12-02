/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

final class SqsPublishBatchSettings private (val concurrentRequests: Int) {

  def withConcurrentRequests(value: Int): SqsPublishBatchSettings = copy(concurrentRequests = value)

  private def copy(concurrentRequests: Int): SqsPublishBatchSettings =
    new SqsPublishBatchSettings(concurrentRequests = concurrentRequests)

  override def toString =
    s"""SqsPublishBatchSettings(concurrentRequests=$concurrentRequests)"""

}

object SqsPublishBatchSettings {

  val Defaults = new SqsPublishBatchSettings(
    concurrentRequests = 1
  )

  /** Scala API */
  def apply(): SqsPublishBatchSettings = Defaults

  /** Java API */
  def create(): SqsPublishBatchSettings = Defaults
}
