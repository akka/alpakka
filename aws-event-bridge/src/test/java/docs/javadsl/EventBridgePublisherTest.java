/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.aws.eventbridge.javadsl.EventBridgePublisher;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;

// #init-client
import java.net.URI;
import com.github.matsluni.akkahttpspi.AkkaHttpClient;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient;
// #init-client

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.services.eventbridge.model.CreateEventBusRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.util.Collections;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class EventBridgePublisherTest {

  static ActorSystem system;
  static Materializer materializer;
  static EventBridgeAsyncClient eventBridgeClient;
  static String eventBusArn;

  static final String endpoint = "http://localhost:4587";

  private static PutEventsRequestEntry detailEntry(String detail) {
    return PutEventsRequestEntry.builder().detail(detail).build();
  }

  private static PutEventsRequest detailPutEventsRequest(String detail) {
    return PutEventsRequest.builder().entries(detailEntry(detail)).build();
  }

  @BeforeClass
  public static void setUpBeforeClass() throws ExecutionException, InterruptedException {
    system = ActorSystem.create("EventBridgePublisherTest");
    materializer = ActorMaterializer.create(system);
    eventBridgeClient = createEventBridgeClient();
    eventBusArn =
        eventBridgeClient
            .createEventBus(
                CreateEventBusRequest.builder()
                    .build()
                    .builder()
                    .name("alpakka-java-eventbus-1")
                    .build())
            .get()
            .eventBusArn();
  }

  @AfterClass
  public static void tearDownAfterClass() {
    TestKit.shutdownActorSystem(system);
  }

  static EventBridgeAsyncClient createEventBridgeClient() {
    // #init-client

    final EventBridgeAsyncClient awsClient =
        EventBridgeAsyncClient.builder()
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
            .endpointOverride(URI.create(endpoint))
            .region(Region.EU_CENTRAL_1)
            .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
            .build();

    system.registerOnTermination(() -> awsClient.close());
    // #init-client

    return awsClient;
  }

  void documentation() {
    // #init-system
    ActorSystem system = ActorSystem.create();
    Materializer materializer = ActorMaterializer.create(system);
    // #init-system
  }

  @Test
  public void sinkShouldPutDetailEntry() throws Exception {
    CompletionStage<Done> completion =
        // #use-sink
        Source.single(detailEntry("message"))
            .runWith(EventBridgePublisher.createSink(eventBridgeClient), materializer);

    // #use-sink
    assertThat(completion.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.getInstance()));
  }

  @Test
  public void sinkShouldPutEventsRequest() throws Exception {
    CompletionStage<Done> completion =
        // #use-sink
        Source.single(detailPutEventsRequest("message"))
            .runWith(EventBridgePublisher.createPublishSink(eventBridgeClient), materializer);

    // #use-sink
    assertThat(completion.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.getInstance()));
  }

  @Test
  public void flowShouldPutDetailEntry() throws Exception {
    CompletionStage<Done> completion =
        // #use-flow
        Source.single(detailEntry("message"))
            .via(EventBridgePublisher.createFlow(eventBridgeClient))
            .runWith(Sink.foreach(res -> System.out.println(res)), materializer);

    // #use-flow
    assertThat(completion.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.getInstance()));
  }

  @Test
  public void flowShouldPutEventsRequest() throws Exception {
    CompletionStage<Done> completion =
        // #use-flow
        Source.single(detailPutEventsRequest("message"))
            .via(EventBridgePublisher.createPublishFlow(eventBridgeClient))
            .runWith(Sink.foreach(res -> System.out.println(res)), materializer);

    // #use-flow
    assertThat(completion.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.getInstance()));
  }
}
