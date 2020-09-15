/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
// #init-system
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
// #init-system
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.*;

import java.util.Collections;
import java.util.UUID;
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
                    .name("alpakka-java-eventbus-" + UUID.randomUUID().toString())
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
        // #run-events-entry
        Source.single(detailEntry("message"))
            .runWith(EventBridgePublisher.sink(eventBridgeClient), materializer);

    // #run-events-entry
    assertThat(completion.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.getInstance()));
  }

  @Test
  public void sinkShouldPutEventsRequest() throws Exception {
    CompletionStage<Done> completion =
        // #run-events-request
        Source.single(detailPutEventsRequest("message"))
            .runWith(EventBridgePublisher.publishSink(eventBridgeClient), materializer);

    // #run-events-request
    assertThat(completion.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.getInstance()));
  }

  @Test
  public void flowShouldPutDetailEntry() throws Exception {
    CompletionStage<Done> completion =
        // #flow-events-entry
        Source.single(detailEntry("message"))
            .via(EventBridgePublisher.flow(eventBridgeClient))
            .runWith(Sink.foreach(res -> System.out.println(res)), materializer);

    // #flow-events-entry
    assertThat(completion.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.getInstance()));
  }

  @Test
  public void flowShouldPutEventsRequest() throws Exception {
    CompletionStage<Done> completion =
        // #flow-events-request
        Source.single(detailPutEventsRequest("message"))
            .via(EventBridgePublisher.publishFlow(eventBridgeClient))
            .runWith(Sink.foreach(res -> System.out.println(res)), materializer);

    // #flow-events-request
    assertThat(completion.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.getInstance()));
  }
}
