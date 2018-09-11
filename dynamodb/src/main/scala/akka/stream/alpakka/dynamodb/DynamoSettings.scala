/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb

import akka.actor.ActorSystem
import com.amazonaws.auth._
import com.typesafe.config.Config

final class DynamoSettings private (
    val region: String,
    val host: String,
    val port: Int,
    val parallelism: Int,
    val credentialsProvider: com.amazonaws.auth.AWSCredentialsProvider
) extends AwsClientSettings {

  require(host.nonEmpty, "A host name must be provided.")
  require(port > -1, "A port number must be provided.")

  def withRegion(value: String): DynamoSettings = copy(region = value)
  def withHost(value: String): DynamoSettings = copy(host = value)
  def withPort(value: Int): DynamoSettings = copy(port = value)
  def withParallelism(value: Int): DynamoSettings = copy(parallelism = value)
  def withCredentialsProvider(value: com.amazonaws.auth.AWSCredentialsProvider): DynamoSettings =
    copy(credentialsProvider = value)

  private def copy(
      region: String = region,
      host: String = host,
      port: Int = port,
      parallelism: Int = parallelism,
      credentialsProvider: com.amazonaws.auth.AWSCredentialsProvider = credentialsProvider
  ): DynamoSettings = new DynamoSettings(
    region = region,
    host = host,
    port = port,
    parallelism = parallelism,
    credentialsProvider = credentialsProvider
  )

  override def toString =
    s"""DynamoSettings(region=$region,host=$host,port=$port,parallelism=$parallelism,credentialsProvider=$credentialsProvider)"""
}

object DynamoSettings {

  /**
   * Scala API: Creates [[DynamoSettings]] from the [[com.typesafe.config.Config Config]] attached to an [[akka.actor.ActorSystem]].
   */
  def apply(system: ActorSystem): DynamoSettings =
    apply(system.settings.config.getConfig("akka.stream.alpakka.dynamodb"))

  /**
   * Scala API: Creates [[DynamoSettings]] from a [[com.typesafe.config.Config Config]]. This config is expected to have
   * been resolved, i.e. already read from `akka.stream.alpakka.dynamodb`
   */
  def apply(c: Config): DynamoSettings = {
    val region = c.getString("region")
    val host = c.getString("host")
    val port = c.getInt("port")
    val parallelism = c.getInt("parallelism")
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
      parallelism,
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
    443,
    4,
    new DefaultAWSCredentialsProviderChain()
  )

  /** Java API */
  def create(
      region: String,
      host: String
  ): DynamoSettings = apply(region, host)
}
