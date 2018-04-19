/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.impl

import akka.actor.ActorSystem
import com.amazonaws.auth._
import com.typesafe.config.Config

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
  def apply(resolvedConfig: Config): DynamoSettings = {
    val awsCredentialsProvider = if (resolvedConfig.hasPath("credentials")) {
      val accessKey = resolvedConfig.getString("credentials.access-key-id")
      val secretKey = resolvedConfig.getString("credentials.secret-key-id")
      new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey))
    } else new DefaultAWSCredentialsProviderChain()

    DynamoSettings(
      region = resolvedConfig.getString("region"),
      host = resolvedConfig.getString("host"),
      port = resolvedConfig.getInt("port"),
      parallelism = resolvedConfig.getInt("parallelism"),
      credentialsProvider = awsCredentialsProvider
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
}

// #init-settings
case class DynamoSettings(region: String,
                          host: String,
                          port: Int,
                          parallelism: Int,
                          credentialsProvider: AWSCredentialsProvider)
// #init-settings
    extends ClientSettings {
  require(host.nonEmpty, "A host name must be provided.")
  require(port > -1, "A port number must be provided.")
}
