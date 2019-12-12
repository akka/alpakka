/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.sns.javadsl.SnsPublisher;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;

// #init-client
import java.net.URI;
import com.github.matsluni.akkahttpspi.AkkaHttpClient;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
// #init-client

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.PublishRequest;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class SnsPublisherTest {

  static ActorSystem system;
  static Materializer materializer;
  static SnsAsyncClient snsClient;
  static String topicArn;

  static final String endpoint = "http://localhost:4100";

  @BeforeClass
  public static void setUpBeforeClass() throws ExecutionException, InterruptedException {
    system = ActorSystem.create("SnsPublisherTest");
    materializer = ActorMaterializer.create(system);
    snsClient = createSnsClient();
    topicArn =
        snsClient
            .createTopic(CreateTopicRequest.builder().name("alpakka-java-topic-1").build())
            .get()
            .topicArn();
  }

  @AfterClass
  public static void tearDownAfterClass() {
    TestKit.shutdownActorSystem(system);
  }

  static SnsAsyncClient createSnsClient() {
    // #init-client

    // Don't encode credentials in your source code!
    // see https://doc.akka.io/docs/alpakka/current/aws-shared-configuration.html
    StaticCredentialsProvider credentialsProvider =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"));
    final SnsAsyncClient awsSnsClient =
        SnsAsyncClient.builder()
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

    system.registerOnTermination(() -> awsSnsClient.close());
    // #init-client

    return awsSnsClient;
  }

  void documentation() {
    // #init-system
    ActorSystem system = ActorSystem.create();
    Materializer materializer = ActorMaterializer.create(system);
    // #init-system
  }

  @Test
  public void sinkShouldPublishString() throws Exception {
    CompletionStage<Done> completion =
        // #use-sink
        Source.single("message")
            .runWith(SnsPublisher.createSink(topicArn, snsClient), materializer);

    // #use-sink
    assertThat(completion.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.getInstance()));
  }

  @Test
  public void sinkShouldPublishRequest() throws Exception {
    CompletionStage<Done> completion =
        // #use-sink
        Source.single(PublishRequest.builder().message("message").build())
            .runWith(SnsPublisher.createPublishSink(topicArn, snsClient), materializer);

    // #use-sink
    assertThat(completion.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.getInstance()));
  }

  @Test
  public void sinkShouldPublishRequestWithDynamicTopic() throws Exception {
    CompletionStage<Done> completion =
        // #use-sink
        Source.single(PublishRequest.builder().message("message").topicArn(topicArn).build())
            .runWith(SnsPublisher.createPublishSink(snsClient), materializer);
    // #use-sink
    assertThat(completion.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.getInstance()));
  }

  @Test
  public void flowShouldPublishString() throws Exception {
    CompletionStage<Done> completion =
        // #use-flow
        Source.single("message")
            .via(SnsPublisher.createFlow(topicArn, snsClient))
            .runWith(Sink.foreach(res -> System.out.println(res.messageId())), materializer);

    // #use-flow
    assertThat(completion.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.getInstance()));
  }

  @Test
  public void flowShouldPublishRequest() throws Exception {
    CompletionStage<Done> completion =
        // #use-flow
        Source.single(PublishRequest.builder().message("message").build())
            .via(SnsPublisher.createPublishFlow(topicArn, snsClient))
            .runWith(Sink.foreach(res -> System.out.println(res.messageId())), materializer);

    // #use-flow
    assertThat(completion.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.getInstance()));
  }

  @Test
  public void flowShouldPublishRequestWithDynamicTopic() throws Exception {
    CompletionStage<Done> completion =
        // #use-flow
        Source.single(PublishRequest.builder().message("message").topicArn(topicArn).build())
            .via(SnsPublisher.createPublishFlow(snsClient))
            .runWith(Sink.foreach(res -> System.out.println(res.messageId())), materializer);

    // #use-flow
    assertThat(completion.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.getInstance()));
  }
}
