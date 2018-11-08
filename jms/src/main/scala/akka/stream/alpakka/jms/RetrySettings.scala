/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms
import scala.concurrent.duration._

object ConnectionRetrySettings {
  def create(): ConnectionRetrySettings = ConnectionRetrySettings()
}

final case class ConnectionRetrySettings(connectTimeout: FiniteDuration = 10.seconds,
                                         initialRetry: FiniteDuration = 100.millis,
                                         backoffFactor: Double = 2,
                                         maxBackoff: FiniteDuration = 1.minute,
                                         maxRetries: Int = 10) {
  def withConnectTimeout(timeout: FiniteDuration): ConnectionRetrySettings = copy(connectTimeout = timeout)
  def withConnectTimeout(timeout: Long, unit: TimeUnit): ConnectionRetrySettings =
    copy(connectTimeout = Duration(timeout, unit))
  def withInitialRetry(delay: FiniteDuration): ConnectionRetrySettings = copy(initialRetry = delay)
  def withInitialRetry(delay: Long, unit: TimeUnit): ConnectionRetrySettings =
    copy(initialRetry = Duration(delay, unit))
  def withBackoffFactor(backoffFactor: Double): ConnectionRetrySettings = copy(backoffFactor = backoffFactor)
  def withMaxBackoff(maxBackoff: FiniteDuration): ConnectionRetrySettings = copy(maxBackoff = maxBackoff)
  def withMaxBackoff(maxBackoff: Long, unit: TimeUnit): ConnectionRetrySettings =
    copy(maxBackoff = Duration(maxBackoff, unit))
  def withMaxRetries(maxRetries: Int): ConnectionRetrySettings = copy(maxRetries = maxRetries)
  def withInfiniteRetries(): ConnectionRetrySettings = withMaxRetries(-1)

  def waitTime(retryNumber: Int): FiniteDuration =
    (initialRetry * Math.pow(retryNumber, backoffFactor)).asInstanceOf[FiniteDuration].min(maxBackoff)
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
