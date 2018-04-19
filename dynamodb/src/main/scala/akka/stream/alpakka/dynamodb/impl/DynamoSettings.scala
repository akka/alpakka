/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb.impl

import akka.actor.ActorSystem
import com.amazonaws.auth._
import com.typesafe.config.Config

import scala.util.Try

object DynamoSettings {
  def apply(system: ActorSystem): DynamoSettings = apply(system.settings.config)

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
}

case class DynamoSettings(region: String,
                          host: String,
                          port: Int,
                          parallelism: Int,
                          credentialsProvider: AWSCredentialsProvider)
    extends ClientSettings {
  require(host.nonEmpty, "A host name must be provided.")
  require(port > -1, "A port number must be provided.")
}
