/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.eventbridge

final class EventBridgePublishSettings private (val concurrency: Int) {
  require(concurrency > 0)

  def withConcurrency(concurrency: Int): EventBridgePublishSettings = copy(concurrency = concurrency)

  def copy(concurrency: Int) = new EventBridgePublishSettings(concurrency)

  override def toString: String =
    "EventBridgePublishSettings(" +
    s"concurrency=$concurrency" +
    ")"
}

object EventBridgePublishSettings {
  val Defaults: EventBridgePublishSettings = new EventBridgePublishSettings(concurrency = 10)

  /** Scala API */
  def apply(): EventBridgePublishSettings = Defaults

  /** Java API */
  def create(): EventBridgePublishSettings = Defaults
}
