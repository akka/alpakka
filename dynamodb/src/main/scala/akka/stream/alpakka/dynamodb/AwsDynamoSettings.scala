/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb
import java.util.Optional
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.alpakka.dynamodb.RetrySettings.{Exponential, Linear}
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.typesafe.config.Config

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration._

final class AwsDynamoSettings private (
    val region: String,
    val host: String,
    val port: Int,
    val tls: Boolean,
    val parallelism: Int,
    val maxOpenRequests: Option[Int],
    override val retrySettings: RetrySettings,
    val credentialsProvider: com.amazonaws.auth.AWSCredentialsProvider
) extends AwsClientSettings {

  require(host.nonEmpty, "A host name must be provided.")
  require(port > -1, "A port number must be provided.")

  def withRegion(value: String): AwsDynamoSettings = copy(region = value)
  def withHost(value: String): AwsDynamoSettings = copy(host = value)
  def withPort(value: Int): AwsDynamoSettings = copy(port = value)
  def withPortOverTls(value: Int): AwsDynamoSettings = copy(port = value, tls = true)
  def withTls(value: Boolean): AwsDynamoSettings =
    if (value == tls) this else copy(tls = value)
  def withParallelism(value: Int): AwsDynamoSettings = copy(parallelism = value)
  def withMaxOpenRequests(value: Option[Int]): AwsDynamoSettings = copy(maxOpenRequests = value)
  def withRetrySettings(value: RetrySettings): AwsDynamoSettings = copy(retrySettings = value)
  def withCredentialsProvider(value: com.amazonaws.auth.AWSCredentialsProvider): AwsDynamoSettings =
    copy(credentialsProvider = value)

  /** Java Api */
  def withMaxOpenRequests(value: Optional[Int]): AwsDynamoSettings = copy(maxOpenRequests = value.asScala)

  /** Java Api */
  def getMaxOpenRequests(): Optional[Int] = maxOpenRequests.asJava

  private def copy(
      region: String = region,
      host: String = host,
      port: Int = port,
      tls: Boolean = tls,
      parallelism: Int = parallelism,
      maxOpenRequests: Option[Int] = maxOpenRequests,
      retrySettings: RetrySettings = retrySettings,
      credentialsProvider: com.amazonaws.auth.AWSCredentialsProvider = credentialsProvider
  ): AwsDynamoSettings = new AwsDynamoSettings(
    region = region,
    host = host,
    port = port,
    tls = tls,
    parallelism = parallelism,
    maxOpenRequests = maxOpenRequests,
    retrySettings = retrySettings,
    credentialsProvider = credentialsProvider
  )

  override def toString =
    s"""AwsDynamoSettings(region=$region,host=$host,port=$port,parallelism=$parallelism,maxOpenRequests=$maxOpenRequests,credentialsProvider=$credentialsProvider)"""
}

object AwsDynamoSettings {

  val ConfigPath = "akka.stream.alpakka.dynamodb"
  val MaxOpenRequestsPath = "max-open-requests"
  val AwsCredentialsAccessKeyPath = "credentials.access-key-id"
  val AwsCredentialsSecretKeyPath = "credentials.secret-key-id"
  val RetryMaximumAttemptsPath = "retry.maximum-attempts"
  val RetryInitialTimeoutPath = "retry.initial-timeout"
  val RetryStrategyPath = "retry.strategy"

  /**
   * Java API: Creates [[AwsDynamoSettings]] from the [[com.typesafe.config.Config Config]] attached to an [[akka.actor.ActorSystem]].
   */
  def create(system: ActorSystem): AwsDynamoSettings = apply(system)

  /**
   * Scala API: Creates [[AwsDynamoSettings]] from the [[com.typesafe.config.Config Config]] attached to an [[akka.actor.ActorSystem]].
   */
  def apply(system: ActorSystem): AwsDynamoSettings =
    apply(system.settings.config.getConfig(ConfigPath))

  /**
   * Scala API: Creates [[AwsDynamoSettings]] from a [[com.typesafe.config.Config Config]]. This config is expected to have
   * been resolved, i.e. already read from `akka.stream.alpakka.dynamodb`
   */
  def apply(c: Config): AwsDynamoSettings = {
    val region = c.getString("region")
    val host = c.getString("host")
    val port = c.getInt("port")
    val tls = c.getBoolean("tls")
    val parallelism = c.getInt("parallelism")
    val maxOpenRequests =
      if (c.hasPath(MaxOpenRequestsPath)) Option(c.getInt(MaxOpenRequestsPath))
      else None

    val awsCredentialsProvider = {
      if (c.hasPath(AwsCredentialsAccessKeyPath) && c.hasPath(AwsCredentialsSecretKeyPath)) {
        val accessKey = c.getString(AwsCredentialsAccessKeyPath)
        val secretKey = c.getString(AwsCredentialsSecretKeyPath)
        new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey))
      } else {
        new DefaultAWSCredentialsProviderChain()
      }
    }

    val retrySettings = {
      if (c.hasPath(RetryMaximumAttemptsPath) && c.hasPath(RetryInitialTimeoutPath) && c.hasPath(RetryStrategyPath)) {
        val maximumAttempts = c.getInt(RetryMaximumAttemptsPath)
        val initialRetryTimeout = c.getDuration(RetryInitialTimeoutPath, TimeUnit.MILLISECONDS).milliseconds
        val backoffStrategy = c.getString(RetryStrategyPath) match {
          case "exponential" => Exponential
          case "linear" => Linear
        }

        RetrySettings(maximumAttempts, initialRetryTimeout, backoffStrategy)
      } else {
        RetrySettings.DefaultRetrySettings
      }
    }

    new AwsDynamoSettings(
      region,
      host,
      port,
      tls,
      parallelism,
      maxOpenRequests,
      retrySettings,
      awsCredentialsProvider
    )
  }

  /**
   * Java API: Creates [[AwsDynamoSettings]] from a [[com.typesafe.config.Config Config]]. This config is expected to have
   * been resolved, i.e. already read from `akka.stream.alpakka.dynamodb`
   */
  def create(resolvedConfig: Config): AwsDynamoSettings = apply(resolvedConfig)

  /** Java API */
  def create(
      region: String,
      host: String
  ): AwsDynamoSettings = apply(region, host)

  /** Scala API */
  def apply(
      region: String,
      host: String
  ): AwsDynamoSettings = new AwsDynamoSettings(
    region,
    host,
    port = 443,
    tls = true,
    parallelism = 4,
    maxOpenRequests = None,
    retrySettings = RetrySettings.DefaultRetrySettings,
    new DefaultAWSCredentialsProviderChain()
  )
}
