/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.stream.alpakka.sqs.Attribute;
import akka.stream.alpakka.sqs.MessageAttributeName;
import akka.stream.alpakka.sqs.SqsSourceSettings;
import akka.stream.alpakka.sqs.javadsl.BaseSqsTest;
import akka.stream.alpakka.sqs.javadsl.SqsSource;
import akka.stream.javadsl.Sink;
import com.github.matsluni.akkahttpspi.AkkaHttpClient;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class SqsSourceTest extends BaseSqsTest {
  static <T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> com) {
    return CompletableFuture.allOf(com.toArray(new CompletableFuture[com.size()]))
        .thenApply(v -> com.stream().map(CompletableFuture::join).collect(Collectors.toList()));
  }

  @Test
  public void streamFromQueue() throws Exception {
    final String queueUrl = randomQueueUrl();

    sequence(
            IntStream.range(0, 100)
                .boxed()
                .map(
                    i ->
                        sqsClient.sendMessage(
                            SendMessageRequest.builder()
                                .queueUrl(queueUrl)
                                .messageBody("alpakka-" + i)
                                .build()))
                .collect(Collectors.toList()))
        .get();

    Thread.sleep(1000); // to let messages arrive (even on Travis)

    final CompletionStage<List<Message>> cs =
        // #run
        SqsSource.create(
                queueUrl,
                SqsSourceSettings.create()
                    .withCloseOnEmptyReceive(true)
                    .withWaitTime(Duration.ofSeconds(0)),
                sqsClient)
            .runWith(Sink.seq(), materializer);
    // #run

    assertEquals(100, cs.toCompletableFuture().get(20, TimeUnit.SECONDS).size());
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
    final SqsAsyncClient customSqsClient =
        SqsAsyncClient.builder()
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
            .endpointOverride(URI.create(sqsEndpoint))
            .region(Region.US_WEST_2)
            // NettyNioAsyncHttpClient.builder().maxConcurrency(100).build()
            .httpClient(AkkaHttpClient.builder().build())
            .build();

    system.registerOnTermination(() -> customSqsClient.close());
    // #init-custom-client

    customSqsClient
        .sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody("alpakka").build())
        .get();

    final CompletionStage<String> cs =
        SqsSource.create(
                queueUrl, SqsSourceSettings.create().withWaitTimeSeconds(0), customSqsClient)
            .map(Message::body)
            .runWith(Sink.head(), materializer);

    assertEquals("alpakka", cs.toCompletableFuture().get(20, TimeUnit.SECONDS));
  }
}
