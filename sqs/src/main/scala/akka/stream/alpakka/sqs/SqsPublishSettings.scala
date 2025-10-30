/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.sqs

final class SqsPublishSettings private (val maxInFlight: Int) {
  require(maxInFlight > 0)

  def withMaxInFlight(maxInFlight: Int): SqsPublishSettings = copy(maxInFlight = maxInFlight)

  private def copy(maxInFlight: Int) = new SqsPublishSettings(maxInFlight)

  override def toString: String =
    "SqsPublishSettings(" +
    s"maxInFlight=$maxInFlight" +
    ")"
}

object SqsPublishSettings {
  val Defaults = new SqsPublishSettings(maxInFlight = 10)

  /**
   * Scala API
   */
  def apply(): SqsPublishSettings = Defaults

  /**
   * Java API
   */
  def create(): SqsPublishSettings = Defaults
}
