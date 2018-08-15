/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.sqs.*;
import akka.stream.alpakka.sqs.javadsl.BaseSqsTest;
import akka.stream.alpakka.sqs.javadsl.SqsSource;
import akka.stream.javadsl.Sink;
import akka.testkit.javadsl.TestKit;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class SqsSourceTest extends BaseSqsTest {

  private final SqsSourceSettings sqsSourceSettings = SqsSourceSettings.create(20, 100, 10);

  @Test
  public void streamFromQueue() throws Exception {

    final String queueUrl = randomQueueUrl();
    sqsClient.sendMessage(queueUrl, "alpakka");

    // #run
    final CompletionStage<String> cs =
        SqsSource.create(queueUrl, sqsSourceSettings, sqsClient)
            .map(Message::getBody)
            .runWith(Sink.head(), materializer);
    // #run

    assertEquals("alpakka", cs.toCompletableFuture().get(10, TimeUnit.SECONDS));
  }

  @Test
  public void settings() throws Exception {
    // #SqsSourceSettings
    SqsSourceSettings settings =
        SqsSourceSettings.Defaults()
            .withWaitTimeSeconds(20)
            .withMaxBufferSize(100)
            .withMaxBatchSize(10)
            .withAttributes(Attribute.senderId(), Attribute.sentTimestamp())
            .withMessageAttributes(MessageAttributeName.create("bar.*"))
            .withCloseOnEmptyReceive();
    // #SqsSourceSettings
    assertEquals(100, settings.maxBufferSize());
  }

  @Test
  public void streamFromQueueWithCustomClient() throws Exception {

    final String queueUrl = randomQueueUrl();

    // #init-custom-client
    AWSCredentialsProvider credentialsProvider =
        new AWSStaticCredentialsProvider(new BasicAWSCredentials("x", "x"));

    AmazonSQSAsync customSqsClient =
        AmazonSQSAsyncClientBuilder.standard()
            .withCredentials(credentialsProvider)
            .withExecutorFactory(() -> Executors.newFixedThreadPool(10))
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(sqsEndpoint, "eu-central-1"))
            .build();
    // #init-custom-client

    sqsClient.sendMessage(queueUrl, "alpakka");

    final CompletionStage<String> cs =
        SqsSource.create(queueUrl, sqsSourceSettings, customSqsClient)
            .map(Message::getBody)
            .runWith(Sink.head(), materializer);

    assertEquals("alpakka", cs.toCompletableFuture().get(10, TimeUnit.SECONDS));
  }
}
