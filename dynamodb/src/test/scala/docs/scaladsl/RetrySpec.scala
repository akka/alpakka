/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.testkit.TestKit
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
// #awsRetryConfiguration
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.internal.retry.SdkDefaultRetrySetting
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy
import software.amazon.awssdk.core.retry.conditions.RetryCondition

// #awsRetryConfiguration
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import org.scalatest.wordspec.AnyWordSpecLike

class RetrySpec extends TestKit(ActorSystem("RetrySpec")) with AnyWordSpecLike with LogCapturing {

  private val credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"))

  // #clientRetryConfig
  implicit val client: DynamoDbAsyncClient = DynamoDbAsyncClient
    .builder()
    .region(Region.AWS_GLOBAL)
    .credentialsProvider(credentialsProvider)
    .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
    // #awsRetryConfiguration
    .overrideConfiguration(
      ClientOverrideConfiguration
        .builder()
        .retryPolicy(
          // This example shows the AWS SDK 2 `RetryPolicy.defaultRetryPolicy()`
          // See https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/core/retry/RetryPolicy.html
          RetryPolicy.builder
            .backoffStrategy(BackoffStrategy.defaultStrategy)
            .throttlingBackoffStrategy(BackoffStrategy.defaultThrottlingStrategy)
            .numRetries(SdkDefaultRetrySetting.defaultMaxAttempts)
            .retryCondition(RetryCondition.defaultRetryCondition)
            .build
        )
        .build()
    )
    // #awsRetryConfiguration
    .build()
  // #clientRetryConfig

  override def afterAll(): Unit = {
    client.close()
    Http(system)
      .shutdownAllConnectionPools()
      .foreach { _ =>
        shutdown()
      }(system.dispatcher)
    super.afterAll()
  }

}
