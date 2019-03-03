/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.stream.alpakka.sqs.*;
import akka.stream.alpakka.sqs.javadsl.BaseSqsTest;
import akka.stream.alpakka.sqs.javadsl.SqsAckFlow;
import akka.stream.alpakka.sqs.javadsl.SqsAckSink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.Sink;
import org.junit.Test;
import scala.Option;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.junit.Assert.assertEquals;

public class SqsAckTest extends BaseSqsTest {

  private List<Message> createMessages() {
    List<Message> messages = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      messages.add(Message.builder().body("test-" + i).build());
    }

    return messages;
  }

  @Test
  public void constructAckSettings() {
    // #SqsAckSettings
    SqsAckSettings sinkSettings = SqsAckSettings.create().withMaxInFlight(10);
    // #SqsAckSettings
    assertEquals(10, sinkSettings.maxInFlight());
  }

  @Test
  public void constructAckBatchSettings() {
    // #SqsAckBatchSettings
    SqsAckBatchSettings flowSettings = SqsAckBatchSettings.create().withConcurrentRequests(1);
    // #SqsAckBatchSettings
    assertEquals(1, flowSettings.concurrentRequests());
  }

  @Test
  public void constructAckGroupedSettings() {
    // #SqsAckGroupedSettings
    SqsAckGroupedSettings flowSettings =
        SqsAckGroupedSettings.create()
            .withMaxBatchSize(10)
            .withMaxBatchWait(Duration.ofMillis(500))
            .withConcurrentRequests(1);
    // #SqsAckGroupedSettings
    assertEquals(10, flowSettings.maxBatchSize());
  }

  @Test
  public void testAcknowledge() throws Exception {
    final String queueUrl = "none";
    SqsAsyncClient awsClient = mock(SqsAsyncClient.class);
    when(awsClient.deleteMessage(any(DeleteMessageRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(DeleteMessageResponse.builder().build()));

    List<Message> messages = createMessages();
    Source<Message, NotUsed> source = Source.fromIterator(messages::iterator);

    CompletionStage<Done> done =
        // #ack
        source
            .map(m -> MessageAction.delete(m))
            .runWith(SqsAckSink.create(queueUrl, SqsAckSettings.create(), awsClient), materializer);
    // #ack

    done.toCompletableFuture().get(1, TimeUnit.SECONDS);
    verify(awsClient, times(messages.size())).deleteMessage(any(DeleteMessageRequest.class));
  }

  @Test
  public void testAcknowledgeViaFlow() throws Exception {
    final String queueUrl = "none";
    SqsAsyncClient awsClient = mock(SqsAsyncClient.class);
    when(awsClient.deleteMessage(any(DeleteMessageRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(DeleteMessageResponse.builder().build()));

    List<Message> messages = createMessages();
    Source<Message, NotUsed> source = Source.fromIterator(messages::iterator);

    CompletionStage<List<SqsAckResult<SqsResponse>>> stage =
        // #flow-ack
        source
            .map(m -> MessageAction.delete(m))
            .via(SqsAckFlow.create(queueUrl, SqsAckSettings.create(), awsClient))
            .runWith(Sink.seq(), materializer);
    // #flow-ack

    List<SqsAckResult<SqsResponse>> result = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertEquals(10, result.size());
    for (int i = 0; i < 10; i++) {
      SqsAckResult r = result.get(i);
      Message m = messages.get(i);
      assertEquals(MessageAction.delete(m), r.messageAction());
    }

    verify(awsClient, times(messages.size())).deleteMessage(any(DeleteMessageRequest.class));
  }

  @Test
  public void testChangeMessageVisibility() throws Exception {
    final String queueUrl = "none";
    SqsAsyncClient awsClient = mock(SqsAsyncClient.class);
    when(awsClient.changeMessageVisibility(any(ChangeMessageVisibilityRequest.class)))
        .thenReturn(
            CompletableFuture.completedFuture(ChangeMessageVisibilityResponse.builder().build()));

    List<Message> messages = createMessages();
    Source<Message, NotUsed> source = Source.fromIterator(messages::iterator);

    CompletionStage<Done> done =
        // #requeue
        source
            .map(m -> MessageAction.changeMessageVisibility(m, 12))
            .runWith(SqsAckSink.create(queueUrl, SqsAckSettings.create(), awsClient), materializer);
    // #requeue
    done.toCompletableFuture().get(1, TimeUnit.SECONDS);

    verify(awsClient, times(messages.size()))
        .changeMessageVisibility(any(ChangeMessageVisibilityRequest.class));
  }

  @Test
  public void testIgnore() throws Exception {
    final String queueUrl = "none";
    SqsAsyncClient awsClient = mock(SqsAsyncClient.class);

    List<Message> messages = createMessages();
    Source<Message, NotUsed> source = Source.fromIterator(messages::iterator);

    CompletionStage<List<SqsAckResult<SqsResponse>>> stage =
        // #ignore
        source
            .map(m -> MessageAction.ignore(m))
            .via(SqsAckFlow.create(queueUrl, SqsAckSettings.create(), awsClient))
            .runWith(Sink.seq(), materializer);
    // #ignore

    List<SqsAckResult<SqsResponse>> result = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertEquals(10, result.size());
    for (int i = 0; i < 10; i++) {
      SqsAckResult r = result.get(i);
      Message m = messages.get(i);

      assertEquals(Option.empty(), r.metadata());
      assertEquals(MessageAction.ignore(m), r.messageAction());
    }
  }

  @Test
  public void testBatchAcknowledge() throws Exception {
    final String queueUrl = "none";

    SqsAsyncClient awsClient = mock(SqsAsyncClient.class);
    final DeleteMessageBatchResponse response = DeleteMessageBatchResponse.builder().build();
    when(awsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(response));

    List<Message> messages = createMessages();
    Source<Message, NotUsed> source = Source.fromIterator(messages::iterator);

    CompletionStage<List<SqsAckResult<SqsResponse>>> stage =
        //  #batch-ack
        source
            .map(m -> MessageAction.delete(m))
            .via(SqsAckFlow.grouped(queueUrl, SqsAckGroupedSettings.create(), awsClient))
            .runWith(Sink.seq(), materializer);

    // #batch-ack

    List<SqsAckResult<SqsResponse>> result = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertEquals(10, result.size());
    for (int i = 0; i < 10; i++) {
      SqsAckResult r = result.get(i);
      Message m = messages.get(i);

      assertEquals(Option.apply(response), r.metadata());
      assertEquals(MessageAction.delete(m), r.messageAction());
    }

    verify(awsClient, times(1)).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
  }

  @Test
  public void testBatchChangeMessageVisibility() throws Exception {
    final String queueUrl = "none";
    SqsAsyncClient awsClient = mock(SqsAsyncClient.class);
    final ChangeMessageVisibilityBatchResponse response =
        ChangeMessageVisibilityBatchResponse.builder().build();
    when(awsClient.changeMessageVisibilityBatch(any(ChangeMessageVisibilityBatchRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(response));

    List<Message> messages = createMessages();
    Source<Message, NotUsed> source = Source.fromIterator(messages::iterator);

    CompletionStage<List<SqsAckResult<SqsResponse>>> stage =
        // #batch-requeue
        source
            .map(m -> MessageAction.changeMessageVisibility(m, 5))
            .via(SqsAckFlow.grouped(queueUrl, SqsAckGroupedSettings.create(), awsClient))
            .runWith(Sink.seq(), materializer);
    // #batch-requeue

    List<SqsAckResult<SqsResponse>> result = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertEquals(10, result.size());
    for (int i = 0; i < 10; i++) {
      SqsAckResult r = result.get(i);
      Message m = messages.get(i);

      assertEquals(Option.apply(response), r.metadata());
      assertEquals(MessageAction.changeMessageVisibility(m, 5), r.messageAction());
    }

    verify(awsClient, times(1))
        .changeMessageVisibilityBatch(any(ChangeMessageVisibilityBatchRequest.class));
  }

  @Test
  public void testBatchIgnore() throws Exception {
    final String queueUrl = "none";
    SqsAsyncClient awsClient = mock(SqsAsyncClient.class);

    List<Message> messages = createMessages();
    Source<Message, NotUsed> source = Source.fromIterator(messages::iterator);

    CompletionStage<List<SqsAckResult<SqsResponse>>> stage =
        // #batch-ignore
        source
            .map(m -> MessageAction.ignore(m))
            .via(SqsAckFlow.grouped(queueUrl, SqsAckGroupedSettings.create(), awsClient))
            .runWith(Sink.seq(), materializer);
    // #batch-ignore

    List<SqsAckResult<SqsResponse>> result = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertEquals(10, result.size());
    for (int i = 0; i < 10; i++) {
      SqsAckResult r = result.get(i);
      Message m = messages.get(i);

      assertEquals(Option.empty(), r.metadata());
      assertEquals(MessageAction.ignore(m), r.messageAction());
    }
  }
}
