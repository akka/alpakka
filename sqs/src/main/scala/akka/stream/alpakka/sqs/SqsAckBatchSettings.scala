/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs
import scala.concurrent.duration.{FiniteDuration, TimeUnit}
import scala.concurrent.duration._
import akka.util.JavaDurationConverters._

final class SqsAckBatchSettings private (val concurrentRequests: Int) {

  require(concurrentRequests > 0)

  def withConcurrentRequests(value: Int): SqsAckBatchSettings = copy(concurrentRequests = value)

  private def copy(concurrentRequests: Int = concurrentRequests): SqsAckBatchSettings =
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
