/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sns

final class SnsPublishSettings private (val concurrency: Int) {
  require(concurrency > 0)

  def withConcurrency(concurrency: Int): SnsPublishSettings = copy(concurrency = concurrency)

  def copy(concurrency: Int) = new SnsPublishSettings(concurrency)

  override def toString: String =
    "SnsPublishSettings(" +
    s"concurrency=$concurrency" +
    ")"
}

object SnsPublishSettings {
  val Defaults: SnsPublishSettings = new SnsPublishSettings(concurrency = 10)

  /** Scala API */
  def apply(): SnsPublishSettings = Defaults

  /** Java API */
  def create(): SnsPublishSettings = Defaults
}
