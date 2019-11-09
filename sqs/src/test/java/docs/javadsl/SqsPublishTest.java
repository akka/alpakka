/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.stream.alpakka.sqs.*;
import akka.stream.alpakka.sqs.javadsl.BaseSqsTest;
import akka.stream.alpakka.sqs.javadsl.SqsPublishFlow;
import akka.stream.alpakka.sqs.javadsl.SqsPublishSink;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.junit.Test;
import software.amazon.awssdk.services.sqs.model.*;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class SqsPublishTest extends BaseSqsTest {

  @Test
  public void constructBatchSettings() {
    // #SqsPublishBatchSettings
    SqsPublishBatchSettings batchSettings =
        SqsPublishBatchSettings.create().withConcurrentRequests(1);
    // #SqsPublishBatchSettings
    assertEquals(1, batchSettings.concurrentRequests());
  }

  @Test
  public void constructGroupedSettings() {
    // #SqsPublishGroupedSettings
    SqsPublishGroupedSettings batchSettings =
        SqsPublishGroupedSettings.create()
            .withMaxBatchSize(10)
            .withMaxBatchWait(Duration.ofMillis(500))
            .withConcurrentRequests(1);
    // #SqsPublishGroupedSettings
    assertEquals(1, batchSettings.concurrentRequests());
  }

  @Test
  public void constructSinkSettings() {
    // #SqsPublishSettings
    SqsPublishSettings sinkSettings = SqsPublishSettings.create().withMaxInFlight(10);
    // #SqsPublishSettings
    assertEquals(10, sinkSettings.maxInFlight());
  }

  @Test
  public void sendToQueue() throws Exception {

    final String queueUrl = randomQueueUrl();

    CompletionStage<Done> done =
        // #run-string
        Source.single("alpakka")
            .runWith(
                SqsPublishSink.create(queueUrl, SqsPublishSettings.create(), sqsClient),
                materializer);
    // #run-string
    done.toCompletableFuture().get(10, TimeUnit.SECONDS);

    List<Message> messages =
        sqsClient
            .receiveMessage(ReceiveMessageRequest.builder().queueUrl(queueUrl).build())
            .get(2, TimeUnit.SECONDS)
            .messages();
    assertEquals(1, messages.size());
    assertEquals("alpakka", messages.get(0).body());
  }

  @Test
  public void sendMessageRequestToQueue() throws Exception {

    final String queueUrl = randomQueueUrl();

    CompletionStage<Done> done =
        // #run-send-request
        // for fix SQS queue
        Source.single(SendMessageRequest.builder().messageBody("alpakka").build())
            .runWith(
                SqsPublishSink.messageSink(queueUrl, SqsPublishSettings.create(), sqsClient),
                materializer);

    // #run-send-request
    done.toCompletableFuture().get(10, TimeUnit.SECONDS);

    List<Message> messages =
        sqsClient
            .receiveMessage(ReceiveMessageRequest.builder().queueUrl(queueUrl).build())
            .get(2, TimeUnit.SECONDS)
            .messages();
    assertEquals(1, messages.size());
    assertEquals("alpakka", messages.get(0).body());
  }

  @Test
  public void sendMessageRequestToQueueWithQueueUrl() throws Exception {

    final String queueUrl = randomQueueUrl();

    CompletionStage<Done> done =
        // #run-send-request
        // for dynamic SQS queues
        Source.single(
                SendMessageRequest.builder().messageBody("alpakka").queueUrl(queueUrl).build())
            .runWith(
                SqsPublishSink.messageSink(SqsPublishSettings.create(), sqsClient), materializer);
    // #run-send-request
    done.toCompletableFuture().get(10, TimeUnit.SECONDS);

    List<Message> messages =
        sqsClient
            .receiveMessage(ReceiveMessageRequest.builder().queueUrl(queueUrl).build())
            .get(2, TimeUnit.SECONDS)
            .messages();
    assertEquals(1, messages.size());
    assertEquals("alpakka", messages.get(0).body());
  }

  @Test
  public void sendViaFlow() throws Exception {
    final String queueUrl = randomQueueUrl();

    CompletionStage<SqsPublishResult> done =
        // #flow
        // for fix SQS queue
        Source.single(SendMessageRequest.builder().messageBody("alpakka-flow").build())
            .via(SqsPublishFlow.create(queueUrl, SqsPublishSettings.create(), sqsClient))
            .runWith(Sink.head(), materializer);

    // #flow
    SqsPublishResult result = done.toCompletableFuture().get(10, TimeUnit.SECONDS);
    assertEquals(toMd5("alpakka-flow"), result.result().md5OfMessageBody());

    List<Message> messages =
        sqsClient
            .receiveMessage(ReceiveMessageRequest.builder().queueUrl(queueUrl).build())
            .get(2, TimeUnit.SECONDS)
            .messages();
    assertEquals(1, messages.size());
    assertEquals("alpakka-flow", messages.get(0).body());
  }

  @Test
  public void sendViaFlowWithDynamicQueue() throws Exception {
    final String queueUrl = randomQueueUrl();

    CompletionStage<SqsPublishResult> done =
        // #flow
        // for dynamic SQS queues
        Source.single(
                SendMessageRequest.builder().messageBody("alpakka-flow").queueUrl(queueUrl).build())
            .via(SqsPublishFlow.create(SqsPublishSettings.create(), sqsClient))
            .runWith(Sink.head(), materializer);
    // #flow
    SqsPublishResult result = done.toCompletableFuture().get(10, TimeUnit.SECONDS);
    assertEquals(toMd5("alpakka-flow"), result.result().md5OfMessageBody());

    List<Message> messages =
        sqsClient
            .receiveMessage(ReceiveMessageRequest.builder().queueUrl(queueUrl).build())
            .get(2, TimeUnit.SECONDS)
            .messages();
    assertEquals(1, messages.size());
    assertEquals("alpakka-flow", messages.get(0).body());
  }

  @Test
  public void sendToQueueWithBatches() throws Exception {
    final String queueUrl = randomQueueUrl();

    // #group
    List<String> messagesToSend = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      messagesToSend.add("message - " + i);
    }

    CompletionStage<Done> done =
        Source.from(messagesToSend)
            .runWith(
                SqsPublishSink.grouped(queueUrl, SqsPublishGroupedSettings.create(), sqsClient),
                materializer);
    // #group
    done.toCompletableFuture().get(10, TimeUnit.SECONDS);

    List<Message> messagesFirstBatch =
        sqsClient
            .receiveMessage(
                ReceiveMessageRequest.builder().maxNumberOfMessages(10).queueUrl(queueUrl).build())
            .get(2, TimeUnit.SECONDS)
            .messages();

    List<Message> messagesSecondBatch =
        sqsClient
            .receiveMessage(
                ReceiveMessageRequest.builder().maxNumberOfMessages(10).queueUrl(queueUrl).build())
            .get(2, TimeUnit.SECONDS)
            .messages();

    assertEquals(20, messagesFirstBatch.size() + messagesSecondBatch.size());
  }

  @Test
  public void sendBatchesToQueue() throws Exception {
    final String queueUrl = randomQueueUrl();

    // #batch-string
    List<String> messagesToSend = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      messagesToSend.add("Message - " + i);
    }
    Iterable<String> it = messagesToSend;

    CompletionStage<Done> done =
        Source.single(it)
            .runWith(
                SqsPublishSink.batch(queueUrl, SqsPublishBatchSettings.create(), sqsClient),
                materializer);
    // #batch-string
    done.toCompletableFuture().get(10, TimeUnit.SECONDS);

    List<Message> messagesFirstBatch =
        sqsClient
            .receiveMessage(
                ReceiveMessageRequest.builder().maxNumberOfMessages(10).queueUrl(queueUrl).build())
            .get(2, TimeUnit.SECONDS)
            .messages();

    assertEquals(10, messagesFirstBatch.size());
  }

  @Test
  public void sendBatchesOfSendMessageRequestsToQueue() throws Exception {
    final String queueUrl = randomQueueUrl();

    // #batch-send-request
    List<SendMessageRequest> messagesToSend = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      messagesToSend.add(SendMessageRequest.builder().messageBody("Message - " + i).build());
    }

    Iterable<SendMessageRequest> it = messagesToSend;

    CompletionStage<Done> done =
        Source.single(it)
            .runWith(
                SqsPublishSink.batchedMessageSink(
                    queueUrl, SqsPublishBatchSettings.create(), sqsClient),
                materializer);
    // #batch-send-request
    done.toCompletableFuture().get(10, TimeUnit.SECONDS);

    List<Message> messagesFirstBatch =
        sqsClient
            .receiveMessage(
                ReceiveMessageRequest.builder().maxNumberOfMessages(10).queueUrl(queueUrl).build())
            .get(2, TimeUnit.SECONDS)
            .messages();

    assertEquals(10, messagesFirstBatch.size());
  }

  @Test
  public void sendMessageWithBatchesAsFlow() throws Exception {
    final String queueUrl = randomQueueUrl();

    Set<SendMessageRequest> messagesToSend = new HashSet<>();
    Set<String> messageMd5s = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      String message = "Message - " + i;
      messagesToSend.add(SendMessageRequest.builder().messageBody(message).build());
      messageMd5s.add(toMd5(message));
    }

    CompletionStage<List<SqsPublishResultEntry>> stage =
        Source.from(messagesToSend)
            .via(SqsPublishFlow.grouped(queueUrl, SqsPublishGroupedSettings.create(), sqsClient))
            .runWith(Sink.seq(), materializer);

    List<SqsPublishResultEntry> results = stage.toCompletableFuture().get(10, TimeUnit.SECONDS);
    assertEquals(10, results.size());
    Set<SendMessageRequest> resultRequests = new HashSet<>();
    Set<String> resultMd5s = new HashSet<>();
    results.forEach(
        e -> {
          resultRequests.add(e.request());
          resultMd5s.add(e.result().md5OfMessageBody());
        });
    assertEquals(messagesToSend, resultRequests);
    assertEquals(messageMd5s, resultMd5s);

    List<Message> messagesFirstBatch =
        sqsClient
            .receiveMessage(
                ReceiveMessageRequest.builder().maxNumberOfMessages(10).queueUrl(queueUrl).build())
            .get(2, TimeUnit.SECONDS)
            .messages();

    assertEquals(10, messagesFirstBatch.size());
  }

  @Test
  public void sendBatchesAsFlow() throws Exception {
    final String queueUrl = randomQueueUrl();

    Set<SendMessageRequest> messagesToSend = new HashSet<>();
    Set<String> messageMd5s = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      String message = "Message - " + i;
      messagesToSend.add(SendMessageRequest.builder().messageBody(message).build());
      messageMd5s.add(toMd5(message));
    }
    Iterable<SendMessageRequest> it = messagesToSend;

    CompletionStage<List<SqsPublishResultEntry>> stage =
        Source.single(it)
            .via(SqsPublishFlow.batch(queueUrl, SqsPublishBatchSettings.create(), sqsClient))
            .mapConcat(x -> x.getEntries())
            .runWith(Sink.seq(), materializer);

    List<SqsPublishResultEntry> results = new ArrayList<>();

    results.addAll(stage.toCompletableFuture().get(1, TimeUnit.SECONDS));
    assertEquals(10, results.size());
    Set<SendMessageRequest> resultRequests = new HashSet<>();
    Set<String> resultMd5s = new HashSet<>();
    results.forEach(
        e -> {
          resultRequests.add(e.request());
          resultMd5s.add(e.result().md5OfMessageBody());
        });
    assertEquals(messagesToSend, resultRequests);
    assertEquals(messageMd5s, resultMd5s);

    List<Message> messagesFirstBatch =
        sqsClient
            .receiveMessage(
                ReceiveMessageRequest.builder().maxNumberOfMessages(10).queueUrl(queueUrl).build())
            .get(2, TimeUnit.SECONDS)
            .messages();

    assertEquals(10, messagesFirstBatch.size());
  }

  @Test
  public void ackViaFlow() throws Exception {
    final String queueUrl = randomQueueUrl();

    CompletionStage<SqsPublishResult> stage =
        Source.single(SendMessageRequest.builder().messageBody("alpakka-flow").build())
            .via(SqsPublishFlow.create(queueUrl, SqsPublishSettings.create(), sqsClient))
            .runWith(Sink.head(), materializer);

    SqsPublishResult result = stage.toCompletableFuture().get(10, TimeUnit.SECONDS);
    assertEquals(toMd5("alpakka-flow"), result.result().md5OfMessageBody());

    List<Message> messages =
        sqsClient
            .receiveMessage(
                ReceiveMessageRequest.builder().maxNumberOfMessages(1).queueUrl(queueUrl).build())
            .get(2, TimeUnit.SECONDS)
            .messages();

    assertEquals(1, messages.size());
    assertEquals("alpakka-flow", messages.get(0).body());
  }

  private String toMd5(String s) throws Exception {
    MessageDigest m = MessageDigest.getInstance("MD5");
    BigInteger bigInt = new BigInteger(1, m.digest(s.getBytes()));
    return String.format("%032x", bigInt);
  }
}
