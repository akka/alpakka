/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.github.matsluni.akkahttpspi.AkkaHttpClient;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.internal.retry.SdkDefaultRetrySetting;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

public class RetryTest {

  @Test
  public void setup() throws Exception {
    final ActorSystem system = ActorSystem.create();
    // #clientRetryConfig
    final DynamoDbAsyncClient client =
        DynamoDbAsyncClient.builder()
            .region(Region.AWS_GLOBAL)
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
            .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
            .overrideConfiguration(
                ClientOverrideConfiguration.builder()
                    .retryPolicy(
                        // This example shows the AWS SDK 2 `RetryPolicy.defaultRetryPolicy()`
                        // See https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/core/retry/RetryPolicy.html
                        RetryPolicy.builder()
                            .backoffStrategy(BackoffStrategy.defaultStrategy())
                            .throttlingBackoffStrategy(BackoffStrategy.defaultThrottlingStrategy())
                            .numRetries(SdkDefaultRetrySetting.DEFAULT_MAX_RETRIES)
                            .retryCondition(RetryCondition.defaultRetryCondition())
                            .build())
                    .build())
            .build();
    // #clientRetryConfig

    client.close();
    TestKit.shutdownActorSystem(system);
  }
}
