/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.internal.retry.SdkDefaultRetrySetting
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy
import software.amazon.awssdk.core.retry.conditions.RetryCondition
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

class RetrySpec extends TestKit(ActorSystem("RetrySpec")) with WordSpecLike with BeforeAndAfterAll {

  // #clientRetryConfig
  implicit val client: DynamoDbAsyncClient = DynamoDbAsyncClient
    .builder()
    .region(Region.AWS_GLOBAL)
    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
    .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
    .overrideConfiguration(
      ClientOverrideConfiguration
        .builder()
        .retryPolicy(
          // This example shows the AWS SDK 2 `RetryPolicy.defaultRetryPolicy()`
          // See https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/core/retry/RetryPolicy.html
          RetryPolicy.builder
            .backoffStrategy(BackoffStrategy.defaultStrategy)
            .throttlingBackoffStrategy(BackoffStrategy.defaultThrottlingStrategy)
            .numRetries(SdkDefaultRetrySetting.DEFAULT_MAX_RETRIES)
            .retryCondition(RetryCondition.defaultRetryCondition)
            .build
        )
        .build()
    )
    .build()
  // #clientRetryConfig

  override def afterAll(): Unit = {
    client.close()
    shutdown();
  }

}
