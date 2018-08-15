/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

final class SqsSinkSettings private (val maxInFlight: Int) {
  require(maxInFlight > 0)

  def withMaxInFlight(maxInFlight: Int): SqsSinkSettings = copy(maxInFlight = maxInFlight)

  private def copy(maxInFlight: Int = maxInFlight) = new SqsSinkSettings(maxInFlight)

  override def toString: String =
    "SqsSinkSettings(" +
    s"maxInFlight=$maxInFlight" +
    ")"
}

object SqsSinkSettings {
  val Defaults = new SqsSinkSettings(maxInFlight = 10)

  /**
   * Scala API
   */
  def apply(): SqsSinkSettings = Defaults

  /**
   * Java API
   */
  def getInstance(): SqsSinkSettings = Defaults

  /**
   * Scala API
   */
  def apply(maxInFlight: Int): SqsSinkSettings = new SqsSinkSettings(maxInFlight)

  /**
   * Java API
   */
  def create(maxInFlight: Int): SqsSinkSettings = new SqsSinkSettings(maxInFlight)
}
