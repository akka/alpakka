/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.eventbridge.javadsl.EventBridgePublisher;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import com.github.matsluni.akkahttpspi.AkkaHttpClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient;
import software.amazon.awssdk.services.eventbridge.model.CreateEventBusRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;

import java.net.URI;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

// #init-client
// #init-client

public class EventBridgePublisherTest {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  static ActorSystem system;
  static Materializer materializer;
  static EventBridgeAsyncClient eventBridgeClient;
  static String eventBusName;
  static String source;
  static String detailType;

  static final String endpoint = "http://localhost:4100";

  @BeforeClass
  public static void setUpBeforeClass() throws ExecutionException, InterruptedException {
    system = ActorSystem.create("SnsPublisherTest");
    materializer = ActorMaterializer.create(system);
    eventBusName = "alpakka-java-event-bus-1";
    source = "source";
    detailType = "detailType";
    eventBridgeClient = createEventBridgeClient();
    eventBridgeClient
        .createEventBus(CreateEventBusRequest.builder().name(eventBusName).build())
        .get();
  }

  @AfterClass
  public static void tearDownAfterClass() {
    TestKit.shutdownActorSystem(system);
  }

  static EventBridgeAsyncClient createEventBridgeClient() {
    // #init-client

    // Don't encode credentials in your source code!
    // see https://doc.akka.io/docs/alpakka/current/aws-shared-configuration.html
    StaticCredentialsProvider credentialsProvider =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"));
    final EventBridgeAsyncClient eventBridgeClient =
        EventBridgeAsyncClient.builder()
            .credentialsProvider(credentialsProvider)
            // #init-client
            .endpointOverride(URI.create(endpoint))
            // #init-client
            .region(Region.EU_CENTRAL_1)
            .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
            // Possibility to configure the retry policy
            // see https://doc.akka.io/docs/alpakka/current/aws-shared-configuration.html
            // .overrideConfiguration(...)
            .build();

    system.registerOnTermination(() -> eventBridgeClient.close());
    // #init-client

    return eventBridgeClient;
  }

  void documentation() {
    // #init-system
    ActorSystem system = ActorSystem.create();
    Materializer materializer = ActorMaterializer.create(system);
    // #init-system
  }

  @Test
  public void sinkShouldPutEventString() throws Exception {
    CompletionStage<Done> completion =
        // #use-sink
        Source.single("message")
            .runWith(
                EventBridgePublisher.createSink(
                    eventBusName, source, detailType, eventBridgeClient),
                materializer);

    // #use-sink
    assertThat(completion.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.getInstance()));
  }

  @Test
  public void sinkShouldPutEventRequestWithDefaultEventBus() throws Exception {
    CompletionStage<Done> completion =
        // #use-sink
        Source.single(
                PutEventsRequest.builder()
                    .entries(
                        PutEventsRequestEntry.builder()
                            .source("source")
                            .detailType("detail-type")
                            .detail("event-detail")
                            .build())
                    .build())
            .runWith(EventBridgePublisher.createPutEventsSink(eventBridgeClient), materializer);

    // #use-sink
    assertThat(completion.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.getInstance()));
  }

  @Test
  public void sinkShouldPutEventRequestWithCustomEventBus() throws Exception {
    CompletionStage<Done> completion =
        // #use-sink
        Source.single(
                PutEventsRequest.builder()
                    .entries(
                        PutEventsRequestEntry.builder()
                            .eventBusName("event-bus")
                            .source("source")
                            .detailType("detail-type")
                            .detail("event-detail")
                            .build())
                    .build())
            .runWith(EventBridgePublisher.createPutEventsSink(eventBridgeClient), materializer);
    // #use-sink
    assertThat(completion.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.getInstance()));
  }

  @Test
  public void flowShouldPutEventString() throws Exception {
    CompletionStage<Done> completion =
        // #use-flow
        Source.single("message")
            .via(
                EventBridgePublisher.createFlow(
                    eventBusName, source, detailType, eventBridgeClient))
            .runWith(
                Sink.foreach(
                    resp -> resp.entries().forEach(res -> System.out.println(res.eventId()))),
                materializer);

    // #use-flow
    assertThat(completion.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.getInstance()));
  }

  @Test
  public void flowShouldPublishRequestWithDefaultEventBus() throws Exception {
    CompletionStage<Done> completion =
        // #use-flow
        Source.single(
                PutEventsRequest.builder()
                    .entries(
                        PutEventsRequestEntry.builder()
                            .source("source")
                            .detailType("detail-type")
                            .detail("event-detail")
                            .build())
                    .build())
            .via(EventBridgePublisher.createPutEventsFlow(eventBridgeClient))
            .runWith(
                Sink.foreach(
                    resp -> resp.entries().forEach(res -> System.out.println(res.eventId()))),
                materializer);

    // #use-flow
    assertThat(completion.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.getInstance()));
  }

  @Test
  public void flowShouldPublishRequestWithCustomEventBus() throws Exception {
    CompletionStage<Done> completion =
        // #use-flow
        Source.single(
                PutEventsRequest.builder()
                    .entries(
                        PutEventsRequestEntry.builder()
                            .eventBusName("event-bus")
                            .source("source")
                            .detailType("detail-type")
                            .detail("event-detail")
                            .build())
                    .build())
            .via(EventBridgePublisher.createPutEventsFlow(eventBridgeClient))
            .runWith(
                Sink.foreach(
                    resp -> resp.entries().forEach(res -> System.out.println(res.eventId()))),
                materializer);

    // #use-flow
    assertThat(completion.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.getInstance()));
  }
}
