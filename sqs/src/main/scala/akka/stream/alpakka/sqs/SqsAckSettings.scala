/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

final class SqsAckSettings private (val maxInFlight: Int) {

  require(maxInFlight > 0)

  def withMaxInFlight(value: Int): SqsAckSettings = copy(maxInFlight = value)

  private def copy(maxInFlight: Int): SqsAckSettings =
    new SqsAckSettings(maxInFlight = maxInFlight)

  override def toString =
    s"""SqsAckSinkSettings(maxInFlight=$maxInFlight)"""
}

object SqsAckSettings {

  val Defaults = new SqsAckSettings(maxInFlight = 10)

  /** Scala API */
  def apply(): SqsAckSettings = Defaults

  /** Java API */
  def create(): SqsAckSettings = Defaults

  /** Scala API */
  def apply(
      maxInFlight: Int
  ): SqsAckSettings = new SqsAckSettings(
    maxInFlight
  )

  /** Java API */
  def create(
      maxInFlight: Int
  ): SqsAckSettings = new SqsAckSettings(
    maxInFlight
  )
}
