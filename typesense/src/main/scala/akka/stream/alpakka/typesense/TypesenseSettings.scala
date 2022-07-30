/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense

import akka.annotation.InternalApi
import scala.jdk.DurationConverters._

import scala.concurrent.duration.FiniteDuration

final class TypesenseSettings @InternalApi private[typesense] (val host: String,
                                                               val apiKey: String,
                                                               val retrySettings: RetrySettings) {

  def withHost(host: String): TypesenseSettings = new TypesenseSettings(host, apiKey, retrySettings)

  def withApiKey(apiKey: String): TypesenseSettings = new TypesenseSettings(host, apiKey, retrySettings)

  def withRetrySettings(retrySettings: RetrySettings): TypesenseSettings =
    new TypesenseSettings(host, apiKey, retrySettings)

  override def equals(other: Any): Boolean = other match {
    case that: TypesenseSettings =>
      host == that.host &&
      apiKey == that.apiKey &&
      retrySettings == that.retrySettings
    case _ => false
  }

  override def hashCode: Int = java.util.Objects.hash(host, apiKey, retrySettings)

  override def toString = s"TypesenseSettings(host=$host, apiKey=***, retrySettings=$retrySettings)"
}

object TypesenseSettings {
  def apply(host: String, apiKey: String, retrySettings: RetrySettings): TypesenseSettings =
    new TypesenseSettings(host, apiKey, retrySettings)

  def create(host: String, apiKey: String, retrySettings: RetrySettings): TypesenseSettings =
    new TypesenseSettings(host, apiKey, retrySettings)
}

final class RetrySettings @InternalApi private[typesense] (val maxRetries: Int,
                                                           val minBackoff: FiniteDuration,
                                                           val maxBackoff: FiniteDuration,
                                                           val randomFactor: Double) {

  def getMinBackoff(): java.time.Duration = minBackoff.toJava

  def getMaxBackoff(): java.time.Duration = maxBackoff.toJava

  override def equals(other: Any): Boolean = other match {
    case that: RetrySettings =>
      maxRetries == that.maxRetries &&
      minBackoff == that.minBackoff &&
      maxBackoff == that.maxBackoff &&
      randomFactor == that.randomFactor
    case _ => false
  }

  override def hashCode: Int = java.util.Objects.hash(maxRetries, minBackoff, maxBackoff, randomFactor)

  override def toString =
    s"RetrySettings(maxRetries=$maxRetries, minBackoff=$minBackoff, maxBackoff=$maxBackoff, randomFactor=$randomFactor)"
}

object RetrySettings {
  def apply(maxRetries: Int,
            minBackoff: FiniteDuration,
            maxBackoff: FiniteDuration,
            randomFactor: Double): RetrySettings =
    new RetrySettings(maxRetries, minBackoff, maxBackoff, randomFactor)

  def create(maxRetries: Int,
             minBackoff: java.time.Duration,
             maxBackoff: java.time.Duration,
             randomFactor: Double): RetrySettings =
    new RetrySettings(maxRetries, minBackoff.toScala, maxBackoff.toScala, randomFactor)
}
