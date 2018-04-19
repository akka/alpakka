/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.impl

import akka.actor.ActorSystem
import com.amazonaws.auth._
import com.typesafe.config.Config

import scala.util.Try

object DynamoSettings {

  /**
   * Scala API: Creates [[DynamoSettings]] from the [[com.typesafe.config.Config Config]] attached to an [[akka.actor.ActorSystem]].
   */
  def apply(system: ActorSystem): DynamoSettings = apply(system.settings.config)

  /**
   * Scala API: Creates [[DynamoSettings]] from a [[com.typesafe.config.Config Config]]
   */
  def apply(baseConfig: Config): DynamoSettings = {
    val config = baseConfig.getConfig("akka.stream.alpakka.dynamodb")
    val awsCredentialsProvider = Try(config.getConfig("credentials"))
      .map { credentialsConfig =>
        val accessKey = credentialsConfig.getString("access-key-id")
        val secretKey = credentialsConfig.getString("secret-key-id")
        new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey))
      }
      .getOrElse(new DefaultAWSCredentialsProviderChain())

    DynamoSettings(
      region = config.getString("region"),
      host = config.getString("host"),
      port = config.getInt("port"),
      parallelism = config.getInt("parallelism"),
      credentialsProvider = awsCredentialsProvider
    )
  }

  /**
   * Java API: Creates [[DynamoSettings]] from the [[com.typesafe.config.Config Config]] attached to an [[akka.actor.ActorSystem]].
   */
  def create(system: ActorSystem): DynamoSettings = apply(system)

  /**
   * Java API: Creates [[DynamoSettings]] from a [[com.typesafe.config.Config Config]]
   */
  def create(config: Config): DynamoSettings = apply(config)
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
