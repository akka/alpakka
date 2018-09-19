/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
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
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sns.AmazonSNSAsync;
import com.amazonaws.services.sns.AmazonSNSAsyncClientBuilder;
// #init-client
import com.amazonaws.services.sns.model.PublishRequest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SnsPublisherTest {

  static ActorSystem system;
  static Materializer materializer;
  static AmazonSNSAsync snsClient;
  static String topicArn;

  static final String endpoint = "http://localhost:4100";

  @BeforeClass
  public static void setUpBeforeClass() {
    system = ActorSystem.create("SnsPublisherTest");
    materializer = ActorMaterializer.create(system);
    snsClient = createSnsClient();
    topicArn = snsClient.createTopic("alpakka-java-topic-1").getTopicArn();
  }

  @AfterClass
  public static void tearDownAfterClass() {
    TestKit.shutdownActorSystem(system);
  }

  static AmazonSNSAsync createSnsClient() {
    // #init-client

    AWSCredentialsProvider credentialsProvider =
        new AWSStaticCredentialsProvider(new BasicAWSCredentials("x", "x"));

    AmazonSNSAsync awsSnsClient =
        AmazonSNSAsyncClientBuilder.standard()
            .withCredentials(credentialsProvider)
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(endpoint, "eu-central-1"))
            .build();
    system.registerOnTermination(() -> awsSnsClient.shutdown());
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
        Source.single(new PublishRequest().withMessage("message"))
            .runWith(SnsPublisher.createPublishSink(topicArn, snsClient), materializer);
    // #use-sink
    assertThat(completion.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.getInstance()));
  }

  @Test
  public void flowShouldPublishString() throws Exception {
    CompletionStage<Done> completion =
        // #use-flow
        Source.single("message")
            .via(SnsPublisher.createFlow(topicArn, snsClient))
            .runWith(Sink.foreach(res -> System.out.println(res.getMessageId())), materializer);

    // #use-flow
    assertThat(completion.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.getInstance()));
  }

  @Test
  public void flowShouldPublishRequest() throws Exception {
    CompletionStage<Done> completion =
        // #use-flow
        Source.single(new PublishRequest().withMessage("message"))
            .via(SnsPublisher.createPublishFlow(topicArn, snsClient))
            .runWith(Sink.foreach(res -> System.out.println(res.getMessageId())), materializer);
    // #use-flow
    assertThat(completion.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.getInstance()));
  }
}
