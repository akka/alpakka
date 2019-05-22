/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb

import java.util.Optional

import akka.actor.ActorSystem
import akka.stream.alpakka.dynamodb.RetrySettings.Exponential
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

class AwsDynamoSettingsSpec extends WordSpecLike with Matchers {

  private val defaultRetrySettings = RetrySettings(3, 1.seconds, Exponential)

  "AwsDynamoSettings" should {
    "create aws dynamo settings" in {
      val provider: AWSCredentialsProvider =
        new AWSStaticCredentialsProvider(new BasicAWSCredentials("accessKey", "secretKey"))
      val settings = AwsDynamoSettings("eu-west-1", "host").withCredentialsProvider(provider)

      settings.region should be("eu-west-1")
      settings.host should be("host")
      settings.port should be(443)
      settings.parallelism should be(4)
      settings.maxOpenRequests should be(None)
      settings.credentialsProvider shouldBe a[AWSStaticCredentialsProvider]
      settings.retrySettings shouldBe defaultRetrySettings
    }

    "read reference config from an actor system" in {
      val system = ActorSystem()
      val settings = AwsDynamoSettings(system)

      settings.region should be("us-east-1")
      settings.host should be("localhost")
      settings.port should be(8001)
      settings.parallelism should be(4)
      settings.maxOpenRequests should be(None)
      settings.credentialsProvider shouldBe a[DefaultAWSCredentialsProviderChain]
      settings.retrySettings shouldBe defaultRetrySettings

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

      val settings = AwsDynamoSettings(config)

      // Test Scala API
      settings.maxOpenRequests shouldBe Some(64)
      settings.withMaxOpenRequests(None).maxOpenRequests shouldBe None
      settings.withMaxOpenRequests(Some(32)).maxOpenRequests shouldBe Some(32)
      settings.retrySettings shouldBe defaultRetrySettings

      // Test Java API
      settings.getMaxOpenRequests shouldBe Optional.of(64)
      settings.withMaxOpenRequests(Optional.empty[Int]).getMaxOpenRequests shouldBe Optional.empty[Int]
      settings.withMaxOpenRequests(Optional.of(32)).getMaxOpenRequests shouldBe Optional.of(32)
      settings.retrySettings shouldBe defaultRetrySettings
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

      val settings = AwsDynamoSettings(config)
      settings.credentialsProvider shouldBe a[DefaultAWSCredentialsProviderChain]
    }

    "read static aws credentials from a config that defines an akka.stream.alpakka.dynamodb.credentials" in {
      val configWithStaticCredentials =
        """
          |akka.stream.alpakka.dynamodb {
          |            region = "eu-west-1"
          |            host = "localhost"
          |            port = 443
          |            tls = true
          |            parallelism = 32
          |            credentials {
          |              access-key-id = "dummy-access-key"
          |              secret-key-id = "dummy-secret-key"
          |            }
          |            retry {
          |              maximum-attempts = 3
          |              initial-timeout = 1s
          |              strategy = exponential
          |            }
          |          }
        """.stripMargin

      val config = ConfigFactory.parseString(configWithStaticCredentials).getConfig("akka.stream.alpakka.dynamodb")
      val settings = AwsDynamoSettings(config)
      val credentials = settings.credentialsProvider.getCredentials

      credentials.getAWSAccessKeyId should be("dummy-access-key")
      credentials.getAWSSecretKey should be("dummy-secret-key")
    }

    "read static retry settings from config at path akka.stream.alpakka.dynamodb.retry" in {
      val staticConfig =
        """
          |akka.stream.alpakka.dynamodb {
          |            region = "eu-west-1"
          |            host = "localhost"
          |            port = 443
          |            tls = true
          |            parallelism = 32
          |            credentials {
          |              access-key-id = "dummy-access-key"
          |              secret-key-id = "dummy-secret-key"
          |            }
          |            retry {
          |              maximum-attempts = 3
          |              initial-timeout = 1s
          |              strategy = exponential
          |            }
          |          }
        """.stripMargin

      val config = ConfigFactory.parseString(staticConfig).getConfig("akka.stream.alpakka.dynamodb")
      val settings = AwsDynamoSettings(config)

      settings.retrySettings.maximumAttempts should be(3)
      settings.retrySettings.initialRetryTimeout should be(1.seconds)
      settings.retrySettings.backoffStrategy should be(Exponential)
    }
  }
}
