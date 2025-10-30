/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package docs.javadsl;

import static org.junit.Assert.assertEquals;

import akka.Done;
import akka.stream.alpakka.sqs.*;
import akka.stream.alpakka.sqs.javadsl.BaseSqsTest;
import akka.stream.alpakka.sqs.javadsl.SqsPublishFlow;
import akka.stream.alpakka.sqs.javadsl.SqsPublishSink;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import software.amazon.awssdk.services.sqs.model.*;

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
                SqsPublishSink.create(queueUrl, SqsPublishSettings.create(), sqsClient), system);
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
                system);

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
            .runWith(SqsPublishSink.messageSink(SqsPublishSettings.create(), sqsClient), system);
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
            .runWith(Sink.head(), system);

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
            .runWith(Sink.head(), system);
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
                system);
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

    CompletionStage<Done> done =
        Source.single(messagesToSend)
            .runWith(
                SqsPublishSink.batch(queueUrl, SqsPublishBatchSettings.create(), sqsClient),
                system);
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

    CompletionStage<Done> done =
        Source.single(messagesToSend)
            .runWith(
                SqsPublishSink.batchedMessageSink(
                    queueUrl, SqsPublishBatchSettings.create(), sqsClient),
                system);
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

    List<SendMessageRequest> messagesToSend = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      messagesToSend.add(SendMessageRequest.builder().messageBody("Message - " + i).build());
    }

    CompletionStage<List<SqsPublishResultEntry>> stage =
        Source.from(messagesToSend)
            .via(SqsPublishFlow.grouped(queueUrl, SqsPublishGroupedSettings.create(), sqsClient))
            .runWith(Sink.seq(), system);

    List<SqsPublishResultEntry> results = stage.toCompletableFuture().get(10, TimeUnit.SECONDS);
    assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) {
      SqsPublishResultEntry r = results.get(i);
      SendMessageRequest req = messagesToSend.get(i);

      assertEquals(req, r.request());
      assertEquals(toMd5(req.messageBody()), r.result().md5OfMessageBody());
    }

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

    List<SendMessageRequest> messagesToSend = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      messagesToSend.add(SendMessageRequest.builder().messageBody("Message - " + i).build());
    }

    CompletionStage<List<SqsPublishResultEntry>> stage =
        Source.single(messagesToSend)
            .via(SqsPublishFlow.batch(queueUrl, SqsPublishBatchSettings.create(), sqsClient))
            .mapConcat(x -> x)
            .runWith(Sink.seq(), system);

    List<SqsPublishResultEntry> results = new ArrayList<>();

    results.addAll(stage.toCompletableFuture().get(1, TimeUnit.SECONDS));
    assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) {
      SqsPublishResultEntry r = results.get(i);
      SendMessageRequest req = messagesToSend.get(i);

      assertEquals(req, r.request());
      assertEquals(toMd5(req.messageBody()), r.result().md5OfMessageBody());
    }

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
            .runWith(Sink.head(), system);

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
