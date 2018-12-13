/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.stream.alpakka.sqs.Attribute;
import akka.stream.alpakka.sqs.MessageAttributeName;
import akka.stream.alpakka.sqs.SqsSourceSettings;
import akka.stream.alpakka.sqs.javadsl.BaseSqsTest;
import akka.stream.alpakka.sqs.javadsl.SqsSource;
import akka.stream.javadsl.Sink;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class SqsSourceTest extends BaseSqsTest {

  @Test
  public void streamFromQueue() throws Exception {

    final String queueUrl = randomQueueUrl();
    for (int i = 0; i < 100; i++) {
      sqsClient.sendMessage(queueUrl, "alpakka-" + i);
    }

    final CompletionStage<List<Message>> cs =
        // #run
        SqsSource.create(
                queueUrl,
                SqsSourceSettings.create()
                    .withCloseOnEmptyReceive(true)
                    .withWaitTime(Duration.ofSeconds(1)),
                sqsClient)
            .runWith(Sink.seq(), materializer);
    // #run

    assertEquals(100, cs.toCompletableFuture().get(10, TimeUnit.SECONDS).size());
  }

  @Test
  public void settings() throws Exception {
    // #SqsSourceSettings
    SqsSourceSettings settings =
        SqsSourceSettings.create()
            .withWaitTime(Duration.ofSeconds(20))
            .withMaxBufferSize(100)
            .withMaxBatchSize(10)
            .withAttributes(Arrays.asList(Attribute.senderId(), Attribute.sentTimestamp()))
            .withMessageAttribute(MessageAttributeName.create("bar.*"))
            .withCloseOnEmptyReceive(true);
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
        SqsSource.create(queueUrl, SqsSourceSettings.create(), customSqsClient)
            .map(Message::getBody)
            .runWith(Sink.head(), materializer);

    assertEquals("alpakka", cs.toCompletableFuture().get(10, TimeUnit.SECONDS));
  }
}
