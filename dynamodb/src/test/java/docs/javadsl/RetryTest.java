/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorSystem;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.testkit.javadsl.TestKit;
import com.github.matsluni.akkahttpspi.AkkaHttpClient;
import org.junit.Rule;
import org.junit.Test;
// #clientRetryConfig
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
// #awsRetryConfiguration
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.internal.retry.SdkDefaultRetrySetting;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
// #clientRetryConfig

// #awsRetryConfiguration

public class RetryTest {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

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
            // #awsRetryConfiguration
            .overrideConfiguration(
                ClientOverrideConfiguration.builder()
                    .retryPolicy(
                        // This example shows the AWS SDK 2 `RetryPolicy.defaultRetryPolicy()`
                        // See
                        // https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/core/retry/RetryPolicy.html
                        RetryPolicy.builder()
                            .backoffStrategy(BackoffStrategy.defaultStrategy())
                            .throttlingBackoffStrategy(BackoffStrategy.defaultThrottlingStrategy())
                            .numRetries(SdkDefaultRetrySetting.DEFAULT_MAX_RETRIES)
                            .retryCondition(RetryCondition.defaultRetryCondition())
                            .build())
                    .build())
            // #awsRetryConfiguration
            .build();
    system.registerOnTermination(client::close);
    // #clientRetryConfig

    client.close();
    TestKit.shutdownActorSystem(system);
  }
}
