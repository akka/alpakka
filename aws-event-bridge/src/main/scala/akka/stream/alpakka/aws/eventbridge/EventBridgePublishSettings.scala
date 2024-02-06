/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.aws.eventbridge

/**
 * Settings of the EventBridgePublish plugin.
 *
 * Currently supports only concurrency parameter which defines how many of the events within the stream would be
 * put into the event bus using the mapAsync method - trying to keep the ordering of the request / entries as they were
 * put into the stream. Use concurrency 1 for having control over failures.
 *
 * @param concurrency maps to parallelism in in async stream operations
 */
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
