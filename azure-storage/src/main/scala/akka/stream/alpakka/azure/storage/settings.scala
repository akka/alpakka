/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage

import akka.actor.ClassicActorSystemProvider
import com.typesafe.config.Config

import java.time.{Duration => JavaDuration}
import java.util.{Objects, Optional}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.jdk.OptionConverters._
import scala.util.Try

final class StorageSettings(val apiVersion: String,
                            val authorizationType: String,
                            val endPointUrl: Option[String],
                            val azureNameKeyCredential: AzureNameKeyCredential,
                            val sasToken: Option[String],
                            val retrySettings: RetrySettings,
                            val algorithm: String) {

  /** Java API */
  def getApiVersion: String = apiVersion

  /** Java API */
  def getAuthorizationType: String = authorizationType

  /** Java API */
  def getEndPointUrl: Optional[String] = endPointUrl.toJava

  /** Java API */
  def getAzureNameKeyCredential: AzureNameKeyCredential = azureNameKeyCredential

  /** Java API */
  def getSasToken: Optional[String] = sasToken.toJava

  /** Java API */
  def getRetrySettings: RetrySettings = retrySettings

  /** Java API */
  def getAlgorithm: String = algorithm

  /** Java API */
  def witApiVersion(apiVersion: String): StorageSettings = copy(apiVersion = apiVersion)

  /** Java API */
  def withAuthorizationType(authorizationType: String): StorageSettings = copy(authorizationType = authorizationType)

  /** Java API */
  def withSasToken(sasToken: String): StorageSettings = copy(sasToken = emptyStringToOption(sasToken))

  /** Java API */
  def withAzureNameKeyCredential(azureNameKeyCredential: AzureNameKeyCredential): StorageSettings =
    copy(azureNameKeyCredential = azureNameKeyCredential)

  /** Java API */
  def withEndPointUrl(endPointUrl: String): StorageSettings = copy(endPointUrl = emptyStringToOption(endPointUrl))

  /** Java API */
  def withRetrySettings(retrySettings: RetrySettings): StorageSettings = copy(retrySettings = retrySettings)

  /** Java API */
  def withAlgorithm(algorithm: String): StorageSettings = copy(algorithm = algorithm)

  override def toString: String =
    s"""StorageSettings(
       | apiVersion=$apiVersion,
       | authorizationType=$authorizationType,
       | azureNameKeyCredential=$azureNameKeyCredential,
       | sasToken=$sasToken
       | retrySettings=$retrySettings,
       | algorithm=$algorithm
       |)""".stripMargin.replaceAll(System.lineSeparator(), "")

  override def equals(other: Any): Boolean = other match {
    case that: StorageSettings =>
      apiVersion == that.apiVersion &&
      authorizationType == that.authorizationType &&
      Objects.equals(azureNameKeyCredential, that.azureNameKeyCredential) &&
      sasToken == that.sasToken &&
      Objects.equals(retrySettings, that.retrySettings) &&
      algorithm == that.algorithm

    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(apiVersion, authorizationType, azureNameKeyCredential, sasToken, retrySettings, algorithm)

  private def copy(
      apiVersion: String = apiVersion,
      authorizationType: String = authorizationType,
      endPointUrl: Option[String] = endPointUrl,
      azureNameKeyCredential: AzureNameKeyCredential = azureNameKeyCredential,
      sasToken: Option[String] = sasToken,
      retrySettings: RetrySettings = retrySettings,
      algorithm: String = algorithm
  ) =
    StorageSettings(apiVersion,
                    authorizationType,
                    endPointUrl,
                    azureNameKeyCredential,
                    sasToken,
                    retrySettings,
                    algorithm)
}

object StorageSettings {
  private[storage] val ConfigPath = "alpakka.azure-storage"
  private val AuthorizationTypes =
    Seq(AnonymousAuthorizationType, SharedKeyAuthorizationType, SharedKeyLiteAuthorizationType, SasAuthorizationType)

  def apply(
      apiVersion: String,
      authorizationType: String,
      endPointUrl: Option[String],
      azureNameKeyCredential: AzureNameKeyCredential,
      sasToken: Option[String],
      retrySettings: RetrySettings,
      algorithm: String
  ): StorageSettings =
    new StorageSettings(apiVersion,
                        authorizationType,
                        endPointUrl,
                        azureNameKeyCredential,
                        sasToken,
                        retrySettings,
                        algorithm)

  /** Java API */
  def create(
      apiVersion: String,
      authorizationType: String,
      endPointUrl: Optional[String],
      azureNameKeyCredential: AzureNameKeyCredential,
      sasToken: Optional[String],
      retrySettings: RetrySettings,
      algorithm: String
  ): StorageSettings =
    StorageSettings(apiVersion,
                    authorizationType,
                    Option(endPointUrl.orElse(null)),
                    azureNameKeyCredential,
                    Option(sasToken.orElse(null)),
                    retrySettings,
                    algorithm)

  def apply(config: Config): StorageSettings = {
    val apiVersion = config.getString("api-version", "2024-11-04")

    val credentials =
      if (config.hasPath("credentials")) config.getConfig("credentials")
      else throw new RuntimeException("credentials must be defined.")

    val authorizationType = {
      val value = credentials.getString("authorization-type", "anon")
      if (AuthorizationTypes.contains(value)) value else AnonymousAuthorizationType
    }

    val retrySettings =
      if (config.hasPath("retry-settings")) RetrySettings(config.getConfig("retry-settings")) else RetrySettings.Default

    StorageSettings(
      apiVersion = apiVersion,
      authorizationType = authorizationType,
      endPointUrl = config.getOptionalString("endpoint-url"),
      azureNameKeyCredential = AzureNameKeyCredential(credentials),
      sasToken = credentials.getOptionalString("sas-token"),
      retrySettings = retrySettings,
      algorithm = config.getString("signing-algorithm", "HmacSHA256")
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
  val Default: RetrySettings = RetrySettings(3, 200.milliseconds, 10.seconds, 0.0)

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

  def apply(config: Config): RetrySettings = {
    Try(
      RetrySettings(
        config.getInt("max-retries"),
        FiniteDuration(config.getDuration("min-backoff").toNanos, TimeUnit.NANOSECONDS),
        FiniteDuration(config.getDuration("max-backoff").toNanos, TimeUnit.NANOSECONDS),
        config.getDouble("random-factor")
      )
    ).toOption.getOrElse(Default)
  }

}
