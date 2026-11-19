/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.maventests;

import static org.junit.Assert.assertTrue;

import akka.actor.ActorSystem;
import com.github.matsluni.akkahttpspi.AkkaHttpClient;
import java.net.ConnectException;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient;

public class AwsEventBridgeTest {

  private static final ActorSystem system = ActorSystem.create("AwsEventBridgeTest");

  @AfterClass
  public static void teardown() {
    system.terminate();
  }

  @Test
  public void callServiceThroughAkkaHttpClient() throws Exception {
    EventBridgeAsyncClient client =
        EventBridgeAsyncClient.builder()
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
            .region(Region.US_EAST_1)
            .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
            .overrideConfiguration(c -> c.retryStrategy(AwsRetryStrategy.doNotRetry()))
            // nothing listens here, the call gets through the SDK and Akka HTTP and fails to connect
            .endpointOverride(new URI("http://localhost:1"))
            .build();

    try {
      client.listEventBuses(b -> {}).get(20, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      Throwable cause = e;
      while (cause.getCause() != null) cause = cause.getCause();
      assertTrue("Unexpected failure: " + cause, cause instanceof ConnectException);
      return;
    } finally {
      client.close();
    }
    throw new AssertionError("Expected the call to fail to connect");
  }
}
