/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb

import akka.actor.ActorSystem
import com.amazonaws.auth.{
  AWSCredentialsProvider,
  AWSStaticCredentialsProvider,
  BasicAWSCredentials,
  DefaultAWSCredentialsProviderChain
}
import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._
import java.util.Optional

class DynamoSettingsSpec extends WordSpecLike with Matchers {

  "DynamoSettings" should {
    "create settings" in {
      // #credentials-provider
      val provider: AWSCredentialsProvider = // ???
        // #credentials-provider
        new AWSStaticCredentialsProvider(new BasicAWSCredentials("accessKey", "secretKey"))
      // #credentials-provider
      val settings = DynamoSettings("eu-west-1", "host")
        .withCredentialsProvider(provider)
      // #credentials-provider

      settings.region should be("eu-west-1")
      settings.host should be("host")
      settings.port should be(443)
      settings.parallelism should be(4)
      settings.maxOpenRequests should be(None)
      settings.credentialsProvider shouldBe a[AWSStaticCredentialsProvider]
    }

    "read the reference config from an actor system" in {
      val system = ActorSystem()
      val settings = DynamoSettings(system)

      settings.region should be("us-east-1")
      settings.host should be("localhost")
      settings.port should be(8001)
      settings.parallelism should be(4)
      settings.maxOpenRequests should be(None)
      settings.credentialsProvider shouldBe a[DefaultAWSCredentialsProviderChain]
      Await.result(system.terminate(), 1.second)
    }

    "allow configuring optional maxOpenRequests" in {
      val config = ConfigFactory.parseString("""
          |region = "eu-west-1"
          |host = "localhost"
          |port = 443
          |tls = true
          |parallelism = 32
          |max-open-requests = 64
        """.stripMargin)

      val settings = DynamoSettings(config)

      // Test Scala API
      settings.maxOpenRequests shouldBe Some(64)
      settings.withMaxOpenRequests(None).maxOpenRequests shouldBe None
      settings.withMaxOpenRequests(Some(32)).maxOpenRequests shouldBe Some(32)

      // Test Java API
      settings.getMaxOpenRequests shouldBe Optional.of(64)
      settings.withMaxOpenRequests(Optional.empty[Int]).getMaxOpenRequests shouldBe Optional.empty[Int]
      settings.withMaxOpenRequests(Optional.of(32)).getMaxOpenRequests shouldBe Optional.of(32)
    }

    "use the DefaultAWSCredentialsProviderChain if the config defines an incomplete akka.stream.alpakka.dynamodb.credentials" in {
      val config = ConfigFactory.parseString("""
          |region = "eu-west-1"
          |host = "localhost"
          |port = 443
          |tls = true
          |parallelism = 32
          |credentials {
          |  access-key-id = "dummy-access-key"
          |}
        """.stripMargin)

      val settings = DynamoSettings(config)
      settings.credentialsProvider shouldBe a[DefaultAWSCredentialsProviderChain]
    }

    "read static aws credentials from a config that defines an akka.stream.alpakka.dynamodb.credentials" in {
      val configWithStaticCredentials =
        """
        // #static-creds
           akka.stream.alpakka.dynamodb {
            region = "eu-west-1"
            host = "localhost"
            port = 443
            tls = true
            parallelism = 32
            credentials {
              access-key-id = "dummy-access-key"
              secret-key-id = "dummy-secret-key"
            }
      // #static-creds
          }""".stripMargin

      val config = ConfigFactory.parseString(configWithStaticCredentials).getConfig("akka.stream.alpakka.dynamodb")

      val settings = DynamoSettings(config)
      val credentials = settings.credentialsProvider.getCredentials
      credentials.getAWSAccessKeyId should be("dummy-access-key")
      credentials.getAWSSecretKey should be("dummy-secret-key")

    }
  }
}
