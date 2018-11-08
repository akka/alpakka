/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms

import scala.concurrent.duration._
import akka.util.JavaDurationConverters._

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

  /** Java API */
  def withConnectTimeout(timeout: Long, unit: java.util.concurrent.TimeUnit): ConnectionRetrySettings =
    copy(connectTimeout = Duration(timeout, unit))

  def withInitialRetry(value: scala.concurrent.duration.FiniteDuration): ConnectionRetrySettings =
    copy(initialRetry = value)

  /** Java API */
  def withInitialRetry(value: java.time.Duration): ConnectionRetrySettings = copy(initialRetry = value.asScala)

  /** Java API */
  def withInitialRetry(delay: Long, unit: java.util.concurrent.TimeUnit): ConnectionRetrySettings =
    copy(initialRetry = Duration(delay, unit))

  def withBackoffFactor(value: Double): ConnectionRetrySettings = copy(backoffFactor = value)

  def withMaxBackoff(value: scala.concurrent.duration.FiniteDuration): ConnectionRetrySettings =
    copy(maxBackoff = value)

  /** Java API */
  def withMaxBackoff(value: java.time.Duration): ConnectionRetrySettings = copy(maxBackoff = value.asScala)

  /** Java API */
  def withMaxBackoff(maxBackoff: Long, unit: java.util.concurrent.TimeUnit): ConnectionRetrySettings =
    copy(maxBackoff = Duration(maxBackoff, unit))

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
    s")"
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
}

object SendRetrySettings {
  def create(): SendRetrySettings = SendRetrySettings()
}

final case class SendRetrySettings(initialRetry: FiniteDuration = 20.millis,
                                   backoffFactor: Double = 1.5,
                                   maxBackoff: FiniteDuration = 500.millis,
                                   maxRetries: Int = 10) {
  def withInitialRetry(delay: FiniteDuration): SendRetrySettings = copy(initialRetry = delay)
  def withInitialRetry(delay: Long, unit: TimeUnit): SendRetrySettings =
    copy(initialRetry = Duration(delay, unit))
  def withBackoffFactor(backoffFactor: Double): SendRetrySettings = copy(backoffFactor = backoffFactor)
  def withMaxBackoff(maxBackoff: FiniteDuration): SendRetrySettings = copy(maxBackoff = maxBackoff)
  def withMaxBackoff(maxBackoff: Long, unit: TimeUnit): SendRetrySettings =
    copy(maxBackoff = Duration(maxBackoff, unit))
  def withMaxRetries(maxRetries: Int): SendRetrySettings = copy(maxRetries = maxRetries)
  def withInfiniteRetries(): SendRetrySettings = withMaxRetries(-1)

  def waitTime(retryNumber: Int): FiniteDuration =
    (initialRetry * Math.pow(retryNumber, backoffFactor)).asInstanceOf[FiniteDuration].min(maxBackoff)
}
