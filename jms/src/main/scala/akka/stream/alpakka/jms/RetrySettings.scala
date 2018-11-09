/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import scala.concurrent.duration._
import akka.util.JavaDurationConverters._
import com.typesafe.config.Config

final class ConnectionRetrySettings private (
    val connectTimeout: scala.concurrent.duration.FiniteDuration,
    val initialRetry: scala.concurrent.duration.FiniteDuration,
    val backoffFactor: Double,
    val maxBackoff: scala.concurrent.duration.FiniteDuration,
    val maxRetries: Int
) {

  def withConnectTimeout(value: scala.concurrent.duration.FiniteDuration): ConnectionRetrySettings =
    copy(connectTimeout = value)

  /** Java API */
  def withConnectTimeout(value: java.time.Duration): ConnectionRetrySettings = copy(connectTimeout = value.asScala)

  def withInitialRetry(value: scala.concurrent.duration.FiniteDuration): ConnectionRetrySettings =
    copy(initialRetry = value)

  /** Java API */
  def withInitialRetry(value: java.time.Duration): ConnectionRetrySettings = copy(initialRetry = value.asScala)

  def withBackoffFactor(value: Double): ConnectionRetrySettings = copy(backoffFactor = value)

  def withMaxBackoff(value: scala.concurrent.duration.FiniteDuration): ConnectionRetrySettings =
    copy(maxBackoff = value)

  /** Java API */
  def withMaxBackoff(value: java.time.Duration): ConnectionRetrySettings = copy(maxBackoff = value.asScala)

  def withMaxRetries(value: Int): ConnectionRetrySettings = copy(maxRetries = value)

  def withInfiniteRetries(): ConnectionRetrySettings = withMaxRetries(-1)

  def waitTime(retryNumber: Int): FiniteDuration =
    (initialRetry * Math.pow(retryNumber, backoffFactor)).asInstanceOf[FiniteDuration].min(maxBackoff)

  private def copy(
      connectTimeout: scala.concurrent.duration.FiniteDuration = connectTimeout,
      initialRetry: scala.concurrent.duration.FiniteDuration = initialRetry,
      backoffFactor: Double = backoffFactor,
      maxBackoff: scala.concurrent.duration.FiniteDuration = maxBackoff,
      maxRetries: Int = maxRetries
  ): ConnectionRetrySettings = new ConnectionRetrySettings(
    connectTimeout = connectTimeout,
    initialRetry = initialRetry,
    backoffFactor = backoffFactor,
    maxBackoff = maxBackoff,
    maxRetries = maxRetries
  )

  override def toString =
    "ConnectionRetrySettings(" +
    s"connectTimeout=${connectTimeout.toCoarsest}," +
    s"initialRetry=${initialRetry.toCoarsest}," +
    s"backoffFactor=$backoffFactor," +
    s"maxBackoff=${maxBackoff.toCoarsest}," +
    s"maxRetries=$maxRetries" +
    ")"
}

object ConnectionRetrySettings {
  private val defaults = new ConnectionRetrySettings(connectTimeout = 10.seconds,
                                                     initialRetry = 100.millis,
                                                     backoffFactor = 2d,
                                                     maxBackoff = 1.minute,
                                                     maxRetries = 10)

  /** Scala API */
  def apply(): ConnectionRetrySettings = defaults

  /** Java API */
  def create(): ConnectionRetrySettings = defaults

  /**
   * Reads from the given config.
   */
  def apply(c: Config): ConnectionRetrySettings = {
    val connectTimeout = c.getDuration("connect-timeout").asScala
    val initialRetry = c.getDuration("initial-retry").asScala
    val backoffFactor = c.getDouble("backoff-factor")
    val maxBackoff = c.getDuration("max-backoff").asScala
    val maxRetries = c.getInt("max-retries")
    new ConnectionRetrySettings(
      connectTimeout,
      initialRetry,
      backoffFactor,
      maxBackoff,
      maxRetries
    )
  }
}

final class SendRetrySettings private (
    val initialRetry: scala.concurrent.duration.FiniteDuration,
    val backoffFactor: Double,
    val maxBackoff: scala.concurrent.duration.FiniteDuration,
    val maxRetries: Int
) {

  /** Scala API */
  def withInitialRetry(value: scala.concurrent.duration.FiniteDuration): SendRetrySettings = copy(initialRetry = value)

  /** Java API */
  def withInitialRetry(value: java.time.Duration): SendRetrySettings = copy(initialRetry = value.asScala)

  def withBackoffFactor(value: Double): SendRetrySettings = copy(backoffFactor = value)

  /** Scala API */
  def withMaxBackoff(value: scala.concurrent.duration.FiniteDuration): SendRetrySettings = copy(maxBackoff = value)

  /** Java API */
  def withMaxBackoff(value: java.time.Duration): SendRetrySettings = copy(maxBackoff = value.asScala)

  def withMaxRetries(value: Int): SendRetrySettings = copy(maxRetries = value)

  def withInfiniteRetries(): SendRetrySettings = withMaxRetries(-1)

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

  override def toString =
    "SendRetrySettings(" +
    s"initialRetry=${initialRetry.toCoarsest}," +
    s"backoffFactor=$backoffFactor," +
    s"maxBackoff=${maxBackoff.toCoarsest}," +
    s"maxRetries=$maxRetries" +
    ")"
}

object SendRetrySettings {

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
    val maxRetries = c.getInt("max-retries")
    new SendRetrySettings(
      initialRetry,
      backoffFactor,
      maxBackoff,
      maxRetries
    )
  }

}
