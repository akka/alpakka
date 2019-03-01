/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb

import akka.actor.ActorSystem
import com.amazonaws.auth._
import com.typesafe.config.Config
import java.util.Optional
import scala.compat.java8.OptionConverters._

final class DynamoSettings private (
    val region: String,
    val host: String,
    val port: Int,
    val tls: Boolean,
    val parallelism: Int,
    val maxOpenRequests: Option[Int],
    val credentialsProvider: com.amazonaws.auth.AWSCredentialsProvider
) extends AwsClientSettings {

  require(host.nonEmpty, "A host name must be provided.")
  require(port > -1, "A port number must be provided.")

  def withRegion(value: String): DynamoSettings = copy(region = value)
  def withHost(value: String): DynamoSettings = copy(host = value)
  def withPort(value: Int): DynamoSettings = copy(port = value)
  def withPortOverTls(value: Int): DynamoSettings = copy(port = value, tls = true)
  def withTls(value: Boolean): DynamoSettings =
    if (value == tls) this else copy(tls = value)
  def withParallelism(value: Int): DynamoSettings = copy(parallelism = value)
  def withMaxOpenRequests(value: Option[Int]): DynamoSettings = copy(maxOpenRequests = value)
  def withCredentialsProvider(value: com.amazonaws.auth.AWSCredentialsProvider): DynamoSettings =
    copy(credentialsProvider = value)

  /** Java Api */
  def withMaxOpenRequests(value: Optional[Int]): DynamoSettings = copy(maxOpenRequests = value.asScala)

  /** Java Api */
  def getMaxOpenRequests(): Optional[Int] = maxOpenRequests.asJava

  private def copy(
      region: String = region,
      host: String = host,
      port: Int = port,
      tls: Boolean = tls,
      parallelism: Int = parallelism,
      maxOpenRequests: Option[Int] = maxOpenRequests,
      credentialsProvider: com.amazonaws.auth.AWSCredentialsProvider = credentialsProvider
  ): DynamoSettings = new DynamoSettings(
    region = region,
    host = host,
    port = port,
    tls = tls,
    parallelism = parallelism,
    maxOpenRequests = maxOpenRequests,
    credentialsProvider = credentialsProvider
  )

  override def toString =
    s"""DynamoSettings(region=$region,host=$host,port=$port,parallelism=$parallelism,maxOpenRequests=$maxOpenRequests,credentialsProvider=$credentialsProvider)"""
}

object DynamoSettings {

  val ConfigPath = "akka.stream.alpakka.dynamodb"

  /**
   * Scala API: Creates [[DynamoSettings]] from the [[com.typesafe.config.Config Config]] attached to an [[akka.actor.ActorSystem]].
   */
  def apply(system: ActorSystem): DynamoSettings =
    apply(system.settings.config.getConfig(ConfigPath))

  /**
   * Scala API: Creates [[DynamoSettings]] from a [[com.typesafe.config.Config Config]]. This config is expected to have
   * been resolved, i.e. already read from `akka.stream.alpakka.dynamodb`
   */
  def apply(c: Config): DynamoSettings = {
    val region = c.getString("region")
    val host = c.getString("host")
    val port = c.getInt("port")
    val tls = c.getBoolean("tls")
    val parallelism = c.getInt("parallelism")
    val maxOpenRequests = if (c.hasPath("max-open-requests")) {
      Option(c.getInt("max-open-requests"))
    } else None

    val awsCredentialsProvider = {
      if (c.hasPath("credentials.access-key-id") &&
          c.hasPath("credentials.secret-key-id")) {
        val accessKey = c.getString("credentials.access-key-id")
        val secretKey = c.getString("credentials.secret-key-id")
        new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey))
      } else new DefaultAWSCredentialsProviderChain()
    }
    new DynamoSettings(
      region,
      host,
      port,
      tls,
      parallelism,
      maxOpenRequests,
      awsCredentialsProvider
    )
  }

  /**
   * Java API: Creates [[DynamoSettings]] from the [[com.typesafe.config.Config Config]] attached to an [[akka.actor.ActorSystem]].
   */
  def create(system: ActorSystem): DynamoSettings = apply(system)

  /**
   * Java API: Creates [[DynamoSettings]] from a [[com.typesafe.config.Config Config]]. This config is expected to have
   * been resolved, i.e. already read from `akka.stream.alpakka.dynamodb`
   */
  def create(resolvedConfig: Config): DynamoSettings = apply(resolvedConfig)

  /** Scala API */
  def apply(
      region: String,
      host: String
  ): DynamoSettings = new DynamoSettings(
    region,
    host,
    port = 443,
    tls = true,
    parallelism = 4,
    maxOpenRequests = None,
    new DefaultAWSCredentialsProviderChain()
  )

  /** Java API */
  def create(
      region: String,
      host: String
  ): DynamoSettings = apply(region, host)
}
