/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import com.typesafe.config.Config

import scala.concurrent.duration._
import akka.util.JavaDurationConverters._

/**
 * When a connection to a broker starts failing, sending JMS messages will also fail.
 * Those failed messages can be retried at the cost of potentially duplicating the failed messages.
 */
final class SendRetrySettings private (val initialRetry: scala.concurrent.duration.FiniteDuration,
                                       val backoffFactor: Double,
                                       val maxBackoff: scala.concurrent.duration.FiniteDuration,
                                       val maxRetries: Int) {

  /** Wait time before retrying the first time. */
  def withInitialRetry(value: scala.concurrent.duration.FiniteDuration): SendRetrySettings = copy(initialRetry = value)

  /** Java API: Wait time before retrying the first time. */
  def withInitialRetry(value: java.time.Duration): SendRetrySettings = copy(initialRetry = value.asScala)

  /** Back-off factor for subsequent retries */
  def withBackoffFactor(value: Double): SendRetrySettings = copy(backoffFactor = value)

  /** Maximum back-off time allowed, after which all retries will happen after this delay. */
  def withMaxBackoff(value: scala.concurrent.duration.FiniteDuration): SendRetrySettings = copy(maxBackoff = value)

  /** Java API: Maximum back-off time allowed, after which all retries will happen after this delay. */
  def withMaxBackoff(value: java.time.Duration): SendRetrySettings = copy(maxBackoff = value.asScala)

  /** Maximum number of retries allowed. */
  def withMaxRetries(value: Int): SendRetrySettings = copy(maxRetries = value)

  /** Do not limit the number of retries. */
  def withInfiniteRetries(): SendRetrySettings = withMaxRetries(SendRetrySettings.infiniteRetries)

  /** The wait time before the next attempt may be made. */
  def waitTime(retryNumber: Int): FiniteDuration =
    (initialRetry * Math.pow(retryNumber, backoffFactor)).asInstanceOf[FiniteDuration].min(maxBackoff)

  private def copy(
      initialRetry: scala.concurrent.duration.FiniteDuration = initialRetry,
      backoffFactor: Double = backoffFactor,
      maxBackoff: scala.concurrent.duration.FiniteDuration = maxBackoff,
      maxRetries: Int = maxRetries
  ): SendRetrySettings = new SendRetrySettings(
    initialRetry = initialRetry,
    backoffFactor = backoffFactor,
    maxBackoff = maxBackoff,
    maxRetries = maxRetries
  )

  override def toString: String =
    "SendRetrySettings(" +
    s"initialRetry=${initialRetry.toCoarsest}," +
    s"backoffFactor=$backoffFactor," +
    s"maxBackoff=${maxBackoff.toCoarsest}," +
    s"maxRetries=${if (maxRetries == SendRetrySettings.infiniteRetries) "infinite" else maxRetries}" +
    ")"
}

object SendRetrySettings {
  val infiniteRetries: Int = -1

  private val defaults =
    new SendRetrySettings(initialRetry = 20.millis, backoffFactor = 1.5, maxBackoff = 500.millis, maxRetries = 10)

  /** Scala API */
  def apply(): SendRetrySettings = defaults

  /** Java API */
  def create(): SendRetrySettings = defaults

  /**
   * Reads from the given config.
   */
  def apply(c: Config): SendRetrySettings = {
    val initialRetry = c.getDuration("initial-retry").asScala
    val backoffFactor = c.getDouble("backoff-factor")
    val maxBackoff = c.getDuration("max-backoff").asScala
    val maxRetries = if (c.getString("max-retries") == "infinite") infiniteRetries else c.getInt("max-retries")
    new SendRetrySettings(
      initialRetry,
      backoffFactor,
      maxBackoff,
      maxRetries
    )
  }

  /**
   * Java API: Reads from given config.
   */
  def create(c: Config): SendRetrySettings = apply(c)

}
