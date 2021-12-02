/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

final class SqsAckBatchSettings private (val concurrentRequests: Int) {

  require(concurrentRequests > 0)

  def withConcurrentRequests(value: Int): SqsAckBatchSettings = copy(concurrentRequests = value)

  private def copy(concurrentRequests: Int): SqsAckBatchSettings =
    new SqsAckBatchSettings(concurrentRequests = concurrentRequests)

  override def toString =
    s"""SqsAckBatchSettings(concurrentRequests=$concurrentRequests)"""

}
object SqsAckBatchSettings {
  val Defaults = new SqsAckBatchSettings(
    concurrentRequests = 1
  )

  /** Scala API */
  def apply(): SqsAckBatchSettings = Defaults

  /** Java API */
  def create(): SqsAckBatchSettings = Defaults
}
