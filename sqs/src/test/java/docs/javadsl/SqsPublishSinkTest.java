/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.stream.alpakka.sqs.SqsAckSettings;
import akka.stream.alpakka.sqs.SqsAckBatchSettings;
import akka.stream.alpakka.sqs.SqsPublishBatchSettings;
import akka.stream.alpakka.sqs.SqsPublishSettings;
import akka.stream.alpakka.sqs.javadsl.BaseSqsTest;
import akka.stream.alpakka.sqs.javadsl.SqsPublishFlow;
import akka.stream.alpakka.sqs.javadsl.SqsPublishSink;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class SqsPublishSinkTest extends BaseSqsTest {

  @Test
  public void constructBatchSettings() {
    // #SqsBatchFlowSettings
    SqsPublishBatchSettings batchSettings =
        SqsPublishBatchSettings.Defaults().withConcurrentRequests(1);
    // #SqsBatchFlowSettings
    assertEquals(1, batchSettings.concurrentRequests());
  }

  @Test
  public void constructSinkSettings() {
    // #SqsSinkSettings
    SqsPublishSettings sinkSettings = SqsPublishSettings.Defaults().withMaxInFlight(10);
    // #SqsSinkSettings
    assertEquals(10, sinkSettings.maxInFlight());
  }

  @Test
  public void constructAckSinkSettings() {
    // #SqsAckSinkSettings
    SqsAckSettings sinkSettings = SqsAckSettings.Defaults().withMaxInFlight(10);
    // #SqsAckSinkSettings
    assertEquals(10, sinkSettings.maxInFlight());
  }

  @Test
  public void constructBatchAckSinkSettings() {
    // #SqsBatchAckFlowSettings
    SqsAckBatchSettings flowSettings =
        SqsAckBatchSettings.Defaults()
            .withMaxBatchSize(10)
            .withMaxBatchWait(Duration.ofMillis(500))
            .withConcurrentRequests(1);
    // #SqsBatchAckFlowSettings
    assertEquals(10, flowSettings.maxBatchSize());
  }

  @Test
  public void sendToQueue() throws Exception {

    final String queueUrl = randomQueueUrl();

    // #run-string
    CompletionStage<Done> done =
        Source.single("alpakka").runWith(SqsPublishSink.create(queueUrl, sqsClient), materializer);
    // #run-string
    done.toCompletableFuture().get(1, TimeUnit.SECONDS);
    List<Message> messages = sqsClient.receiveMessage(queueUrl).getMessages();

    assertEquals(1, messages.size());
    assertEquals("alpakka", messages.get(0).getBody());
  }

  @Test
  public void sendMessageRequestToQueue() throws Exception {

    final String queueUrl = randomQueueUrl();

    // #run-send-request
    CompletionStage<Done> done =
        Source.single(new SendMessageRequest().withMessageBody("alpakka"))
            .runWith(SqsPublishSink.messageSink(queueUrl, sqsClient), materializer);
    // #run-send-request
    done.toCompletableFuture().get(1, TimeUnit.SECONDS);
    List<Message> messages = sqsClient.receiveMessage(queueUrl).getMessages();

    assertEquals(1, messages.size());
    assertEquals("alpakka", messages.get(0).getBody());
  }

  @Test
  public void sendViaFlow() throws Exception {
    final String queueUrl = randomQueueUrl();

    // #flow
    CompletionStage<Done> done =
        Source.single(new SendMessageRequest(queueUrl, "alpakka-flow"))
            .via(SqsPublishFlow.create(queueUrl, sqsClient))
            .runWith(Sink.ignore(), materializer);
    // #flow
    done.toCompletableFuture().get(1, TimeUnit.SECONDS);
    List<Message> messages = sqsClient.receiveMessage(queueUrl).getMessages();

    assertEquals(1, messages.size());
    assertEquals("alpakka-flow", messages.get(0).getBody());
  }

  @Test
  public void sendToQueueWithBatches() throws Exception {
    final String queueUrl = randomQueueUrl();

    // #group
    ArrayList<String> messagesToSend = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      messagesToSend.add("message - " + i);
    }

    CompletionStage<Done> done =
        Source.from(messagesToSend)
            .runWith(SqsPublishSink.grouped(queueUrl, sqsClient), materializer);
    // #group
    done.toCompletableFuture().get(1, TimeUnit.SECONDS);

    List<Message> messagesFirstBatch =
        sqsClient
            .receiveMessage(
                new ReceiveMessageRequest().withQueueUrl(queueUrl).withMaxNumberOfMessages(10))
            .getMessages();
    List<Message> messagesSecondBatch =
        sqsClient
            .receiveMessage(
                new ReceiveMessageRequest().withQueueUrl(queueUrl).withMaxNumberOfMessages(10))
            .getMessages();

    assertEquals(20, messagesFirstBatch.size() + messagesSecondBatch.size());
  }

  @Test
  public void sendBatchesToQueue() throws Exception {
    final String queueUrl = randomQueueUrl();

    // #batch-string
    ArrayList<String> messagesToSend = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      messagesToSend.add("Message - " + i);
    }
    Iterable<String> it = messagesToSend;

    CompletionStage<Done> done =
        Source.single(it).runWith(SqsPublishSink.batch(queueUrl, sqsClient), materializer);
    // #batch-string
    done.toCompletableFuture().get(1, TimeUnit.SECONDS);

    List<Message> messagesFirstBatch =
        sqsClient
            .receiveMessage(
                new ReceiveMessageRequest().withQueueUrl(queueUrl).withMaxNumberOfMessages(10))
            .getMessages();

    assertEquals(10, messagesFirstBatch.size());
  }

  @Test
  public void sendBatchesOfSendMessageRequestsToQueue() throws Exception {
    final String queueUrl = randomQueueUrl();

    // #batch-send-request
    ArrayList<SendMessageRequest> messagesToSend = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      messagesToSend.add(new SendMessageRequest().withMessageBody("Message - " + i));
    }
    Iterable<SendMessageRequest> it = messagesToSend;

    CompletionStage<Done> done =
        Source.single(it)
            .runWith(SqsPublishSink.batchedMessageSink(queueUrl, sqsClient), materializer);
    // #batch-send-request
    done.toCompletableFuture().get(1, TimeUnit.SECONDS);

    List<Message> messagesFirstBatch =
        sqsClient
            .receiveMessage(
                new ReceiveMessageRequest().withQueueUrl(queueUrl).withMaxNumberOfMessages(10))
            .getMessages();

    assertEquals(10, messagesFirstBatch.size());
  }

  @Test
  public void sendMessageWithBatchesAsFlow() throws Exception {
    final String queueUrl = randomQueueUrl();

    ArrayList<SendMessageRequest> messagesToSend = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      messagesToSend.add(new SendMessageRequest(queueUrl, "Message - " + i));
    }

    CompletionStage<Done> done =
        Source.from(messagesToSend)
            .via(SqsPublishFlow.grouped(queueUrl, sqsClient))
            .runWith(Sink.ignore(), materializer);

    done.toCompletableFuture().get(1, TimeUnit.SECONDS);

    List<Message> messagesFirstBatch =
        sqsClient
            .receiveMessage(
                new ReceiveMessageRequest().withQueueUrl(queueUrl).withMaxNumberOfMessages(10))
            .getMessages();

    assertEquals(10, messagesFirstBatch.size());
  }

  @Test
  public void sendBatchesAsFlow() throws Exception {
    final String queueUrl = randomQueueUrl();

    ArrayList<SendMessageRequest> messagesToSend = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      messagesToSend.add(new SendMessageRequest(queueUrl, "Message - " + i));
    }
    Iterable<SendMessageRequest> it = messagesToSend;

    CompletionStage<Done> done =
        Source.single(it)
            .via(SqsPublishFlow.batch(queueUrl, sqsClient))
            .runWith(Sink.ignore(), materializer);

    done.toCompletableFuture().get(1, TimeUnit.SECONDS);

    List<Message> messagesFirstBatch =
        sqsClient
            .receiveMessage(
                new ReceiveMessageRequest().withQueueUrl(queueUrl).withMaxNumberOfMessages(10))
            .getMessages();

    assertEquals(10, messagesFirstBatch.size());
  }

  @Test
  public void ackViaFlow() throws Exception {
    final String queueUrl = randomQueueUrl();

    CompletionStage<Done> done =
        Source.single(new SendMessageRequest(queueUrl, "alpakka-flow"))
            .via(SqsPublishFlow.create(queueUrl, sqsClient))
            .runWith(Sink.ignore(), materializer);
    done.toCompletableFuture().get(1, TimeUnit.SECONDS);
    List<Message> messages = sqsClient.receiveMessage(queueUrl).getMessages();

    assertEquals(1, messages.size());
    assertEquals("alpakka-flow", messages.get(0).getBody());
  }
}
