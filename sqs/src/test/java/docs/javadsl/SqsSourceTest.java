/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.stream.alpakka.sqs.MessageAttributeName;
import akka.stream.alpakka.sqs.MessageSystemAttributeName;
import akka.stream.alpakka.sqs.SqsPublishBatchSettings;
import akka.stream.alpakka.sqs.SqsSourceSettings;
import akka.stream.alpakka.sqs.javadsl.BaseSqsTest;
import akka.stream.alpakka.sqs.javadsl.SqsPublishSink;
import akka.stream.alpakka.sqs.javadsl.SqsSource;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.github.matsluni.akkahttpspi.AkkaHttpClient;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class SqsSourceTest extends BaseSqsTest {

  @Test
  public void streamFromQueue() throws Exception {
    final String queueUrl = randomQueueUrl();

    SqsPublishBatchSettings batchSettings = SqsPublishBatchSettings.create();

    CompletionStage<Done> produced =
        Source.fromIterator(() -> IntStream.range(0, 100).boxed().iterator())
            .map(
                i ->
                    SendMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .messageBody("alpakka-" + i)
                        .build())
            .grouped(10)
            .runWith(
                SqsPublishSink.batchedMessageSink(queueUrl, batchSettings, sqsClient),
                materializer);

    produced.toCompletableFuture().get(2, TimeUnit.SECONDS);

    // #run
    final CompletionStage<List<Message>> messages =
        SqsSource.create(
                queueUrl,
                SqsSourceSettings.create()
                    .withCloseOnEmptyReceive(true)
                    .withWaitTime(Duration.ofMillis(10)),
                sqsClient)
            .runWith(Sink.seq(), materializer);
    // #run

    assertEquals(100, messages.toCompletableFuture().get(20, TimeUnit.SECONDS).size());
  }

  @Test
  public void settings() throws Exception {
    // #SqsSourceSettings
    SqsSourceSettings settings =
        SqsSourceSettings.create()
            .withWaitTime(Duration.ofSeconds(20))
            .withMaxBufferSize(100)
            .withMaxBatchSize(10)
            .withAttributes(
                Arrays.asList(
                    MessageSystemAttributeName.senderId(),
                    MessageSystemAttributeName.sentTimestamp()))
            .withMessageAttribute(MessageAttributeName.create("bar.*"))
            .withCloseOnEmptyReceive(true);
    // #SqsSourceSettings
    assertEquals(100, settings.maxBufferSize());
  }

  @Test
  public void streamFromQueueWithCustomClient() throws Exception {

    final String queueUrl = randomQueueUrl();

    /*
    // #init-custom-client
    import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
    SdkAsyncHttpClient customClient = NettyNioAsyncHttpClient.builder().maxConcurrency(100).build();
    // #init-custom-client
    */
    SdkAsyncHttpClient customClient = AkkaHttpClient.builder().withActorSystem(system).build();
    // #init-custom-client
    final SqsAsyncClient customSqsClient =
        SqsAsyncClient.builder()
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
            .endpointOverride(URI.create(sqsEndpoint))
            .region(Region.US_WEST_2)
            .httpClient(customClient)
            .build();

    system.registerOnTermination(() -> customSqsClient.close());
    // #init-custom-client

    customSqsClient
        .sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody("alpakka").build())
        .get(2, TimeUnit.SECONDS);

    final CompletionStage<String> cs =
        SqsSource.create(
                queueUrl, SqsSourceSettings.create().withWaitTimeSeconds(0), customSqsClient)
            .map(Message::body)
            .runWith(Sink.head(), materializer);

    assertEquals("alpakka", cs.toCompletableFuture().get(20, TimeUnit.SECONDS));
  }
}
