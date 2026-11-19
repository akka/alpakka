/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.maventests;

import static org.junit.Assert.assertTrue;

import akka.actor.ActorSystem;
import akka.grpc.GrpcClientSettings;
import com.google.pubsub.v1.*;
import java.net.ConnectException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.Test;

public class GoogleCloudPubSubGrpcTest {

  private static final ActorSystem system = ActorSystem.create("GoogleCloudPubSubGrpcTest");

  @AfterClass
  public static void teardown() {
    system.terminate();
  }

  @Test
  public void callServiceThroughGeneratedClient() throws Exception {
    // nothing listens here, the call gets through the generated client and gRPC and fails to connect
    GrpcClientSettings settings =
        GrpcClientSettings.connectToServiceAt("localhost", 1, system)
            .withTls(false)
            .withDeadline(Duration.ofSeconds(10));
    PublisherClient client = PublisherClient.create(settings, system);

    try {
      client.listTopics(ListTopicsRequest.newBuilder().setProject("projects/test").build()).toCompletableFuture().get(20, TimeUnit.SECONDS);
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
