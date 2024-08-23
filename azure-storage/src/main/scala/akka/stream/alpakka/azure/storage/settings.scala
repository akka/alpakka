/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage

import akka.actor.ClassicActorSystemProvider
import com.typesafe.config.Config

import java.time.{Duration => JavaDuration}
import java.util.Objects
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

final class StorageSettings(val apiVersion: String,
                            val authorizationType: String,
                            val azureNameKeyCredential: AzureNameKeyCredential,
                            val retrySettings: RetrySettings,
                            val algorithm: String) {

  /** Java API */
  def getApiVersion: String = apiVersion

  /** Java API */
  def getAuthorizationType: String = authorizationType

  /** Java API */
  def getAzureNameKeyCredential: AzureNameKeyCredential = azureNameKeyCredential

  /** Java API */
  def getRetrySettings: RetrySettings = retrySettings

  /** Java API */
  def getAlgorithm: String = algorithm

  /** Java API */
  def witApiVersion(apiVersion: String): StorageSettings = copy(apiVersion = apiVersion)

  /** Java API */
  def withAuthorizationType(authorizationType: String): StorageSettings = copy(authorizationType = authorizationType)

  /** Java API */
  def withAzureNameKeyCredential(azureNameKeyCredential: AzureNameKeyCredential): StorageSettings =
    copy(azureNameKeyCredential = azureNameKeyCredential)

  /** Java API */
  def withRetrySettings(retrySettings: RetrySettings): StorageSettings = copy(retrySettings = retrySettings)

  /** Java API */
  def withAlgorithm(algorithm: String): StorageSettings = copy(algorithm = algorithm)

  override def toString: String =
    s"""StorageSettings(
       | apiVersion=$apiVersion,
       | authorizationType=$authorizationType,
       | azureNameKeyCredential=$azureNameKeyCredential,
       | retrySettings=$retrySettings,
       | algorithm=$algorithm
       |)""".stripMargin.replaceAll(System.lineSeparator(), "")

  override def equals(other: Any): Boolean = other match {
    case that: StorageSettings =>
      apiVersion == that.apiVersion &&
      authorizationType == that.authorizationType &&
      Objects.equals(azureNameKeyCredential, that.azureNameKeyCredential) &&
      Objects.equals(retrySettings, that.retrySettings) &&
      algorithm == that.algorithm

    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(apiVersion, authorizationType, azureNameKeyCredential, retrySettings, algorithm)

  private def copy(
      apiVersion: String = apiVersion,
      authorizationType: String = authorizationType,
      azureNameKeyCredential: AzureNameKeyCredential = azureNameKeyCredential,
      retrySettings: RetrySettings = retrySettings,
      algorithm: String = algorithm
  ) =
    StorageSettings(apiVersion, authorizationType, azureNameKeyCredential, retrySettings, algorithm)
}

object StorageSettings {
  private[storage] val ConfigPath = "alpakka.azure-storage"

  def apply(
      apiVersion: String,
      authorizationType: String,
      azureNameKeyCredential: AzureNameKeyCredential,
      retrySettings: RetrySettings,
      algorithm: String
  ): StorageSettings =
    new StorageSettings(apiVersion, authorizationType, azureNameKeyCredential, retrySettings, algorithm)

  /** Java API */
  def create(
      apiVersion: String,
      authorizationType: String,
      azureNameKeyCredential: AzureNameKeyCredential,
      retrySettings: RetrySettings,
      algorithm: String
  ): StorageSettings =
    StorageSettings(apiVersion, authorizationType, azureNameKeyCredential, retrySettings, algorithm)

  def apply(config: Config): StorageSettings = {
    val apiVersion = config.getString("api-version")
    val credentials = config.getConfig("credentials")
    val authorizationType = credentials.getString("authorization-type")
    val algorithm = config.getString("signing-algorithm")

    StorageSettings(
      apiVersion = apiVersion,
      authorizationType = authorizationType,
      azureNameKeyCredential = AzureNameKeyCredential(credentials),
      retrySettings = RetrySettings(config.getConfig("retry-settings")),
      algorithm = algorithm
    )
  }

  def apply(system: ClassicActorSystemProvider): StorageSettings =
    StorageSettings(system.classicSystem.settings.config.getConfig(ConfigPath))
}

final class RetrySettings private (val maxRetries: Int,
                                   val minBackoff: FiniteDuration,
                                   val maxBackoff: FiniteDuration,
                                   val randomFactor: Double) {

  /** Java API */
  def getMaxRetries: Int = maxRetries

  /** Java API */
  def getMinBackoff: JavaDuration = JavaDuration.ofNanos(minBackoff.toNanos)

  /** Java API */
  def getMaxBackoff: JavaDuration = JavaDuration.ofNanos(maxBackoff.toNanos)

  /** Java API */
  def getRandomFactor: Double = randomFactor

  def withMaxRetries(value: Int): RetrySettings = copy(maxRetries = value)

  def withMinBackoff(value: FiniteDuration): RetrySettings = copy(minBackoff = value)

  /** Java API */
  def withMinBackoff(value: JavaDuration): RetrySettings =
    copy(minBackoff = FiniteDuration(value.toNanos, TimeUnit.NANOSECONDS))

  def withMaxBackoff(value: FiniteDuration): RetrySettings = copy(maxBackoff = value)

  /** Java API */
  def withMaxBackoff(value: JavaDuration): RetrySettings =
    copy(maxBackoff = FiniteDuration(value.toNanos, TimeUnit.NANOSECONDS))

  def withRandomFactor(value: Double): RetrySettings = copy(randomFactor = value)

  private def copy(
      maxRetries: Int = maxRetries,
      minBackoff: FiniteDuration = minBackoff,
      maxBackoff: FiniteDuration = maxBackoff,
      randomFactor: Double = randomFactor
  ) =
    new RetrySettings(maxRetries, minBackoff, maxBackoff, randomFactor)

  override def toString: String =
    "RetrySettings(" +
    s"maxRetries=$maxRetries," +
    s"minBackoff=$minBackoff," +
    s"maxBackoff=$maxBackoff," +
    s"randomFactor=$randomFactor)"

  override def equals(other: Any): Boolean = other match {
    case that: RetrySettings =>
      Objects.equals(this.maxRetries, that.maxRetries) &&
      Objects.equals(this.minBackoff, that.minBackoff) &&
      Objects.equals(this.maxBackoff, that.maxBackoff) &&
      Objects.equals(this.randomFactor, that.randomFactor)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(Int.box(maxRetries), minBackoff, maxBackoff, Double.box(randomFactor))
}

object RetrySettings {
  val default: RetrySettings = RetrySettings(3, 200.milliseconds, 10.seconds, 0.0)

  /** Scala API */
  def apply(
      maxRetries: Int,
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double
  ): RetrySettings =
    new RetrySettings(maxRetries, minBackoff, maxBackoff, randomFactor)

  /** Java API */
  def create(maxRetries: Int, minBackoff: JavaDuration, maxBackoff: JavaDuration, randomFactor: Double): RetrySettings =
    apply(
      maxRetries,
      FiniteDuration(minBackoff.toNanos, TimeUnit.NANOSECONDS),
      FiniteDuration(maxBackoff.toNanos, TimeUnit.NANOSECONDS),
      randomFactor
    )

  def apply(config: Config): RetrySettings =
    RetrySettings(
      config.getInt("max-retries"),
      FiniteDuration(config.getDuration("min-backoff").toNanos, TimeUnit.NANOSECONDS),
      FiniteDuration(config.getDuration("max-backoff").toNanos, TimeUnit.NANOSECONDS),
      config.getDouble("random-factor")
    )
}