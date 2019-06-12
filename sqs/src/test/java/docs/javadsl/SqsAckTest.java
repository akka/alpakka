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
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
    List<Message> messages = createMessages();
    DeleteMessageResponse response = DeleteMessageResponse.builder().build();

    SqsAsyncClient awsClient = mock(SqsAsyncClient.class);
    when(awsClient.deleteMessage(any(DeleteMessageRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(response));

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
    List<Message> messages = createMessages();
    DeleteMessageResponse response = DeleteMessageResponse.builder().build();

    SqsAsyncClient awsClient = mock(SqsAsyncClient.class);
    when(awsClient.deleteMessage(any(DeleteMessageRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(response));

    Source<Message, NotUsed> source = Source.fromIterator(messages::iterator);
    CompletionStage<List<SqsAckResult>> stage =
        // #flow-ack
        source
            .map(m -> MessageAction.delete(m))
            .via(SqsAckFlow.create(queueUrl, SqsAckSettings.create(), awsClient))
            .runWith(Sink.seq(), materializer);
    // #flow-ack

    List<SqsAckResult> results = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) {
      SqsAckResult r = results.get(i);
      Message m = messages.get(i);

      MessageAction messageAction = MessageAction.delete(m);
      assertEquals(messageAction, r.messageAction());
      assertEquals(response, r.result());
    }

    verify(awsClient, times(messages.size())).deleteMessage(any(DeleteMessageRequest.class));
  }

  @Test
  public void testChangeMessageVisibility() throws Exception {
    final String queueUrl = "none";
    List<Message> messages = createMessages();
    ChangeMessageVisibilityResponse response = ChangeMessageVisibilityResponse.builder().build();

    SqsAsyncClient awsClient = mock(SqsAsyncClient.class);
    when(awsClient.changeMessageVisibility(any(ChangeMessageVisibilityRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(response));

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
    List<Message> messages = createMessages();

    SqsAsyncClient awsClient = mock(SqsAsyncClient.class);

    Source<Message, NotUsed> source = Source.fromIterator(messages::iterator);
    CompletionStage<List<SqsAckResult>> stage =
        // #ignore
        source
            .map(m -> MessageAction.ignore(m))
            .via(SqsAckFlow.create(queueUrl, SqsAckSettings.create(), awsClient))
            .runWith(Sink.seq(), materializer);
    // #ignore

    List<SqsAckResult> results = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) {
      SqsAckResult r = results.get(i);
      Message m = messages.get(i);

      MessageAction messageAction = MessageAction.ignore(m);
      assertEquals(messageAction, r.messageAction());
      assertEquals(NotUsed.getInstance(), r.result());
    }
  }

  @Test
  public void testBatchAcknowledge() throws Exception {
    final String queueUrl = "none";
    List<Message> messages = createMessages();
    List<DeleteMessageBatchResultEntry> entries =
        IntStream.range(0, messages.size())
            .mapToObj(i -> DeleteMessageBatchResultEntry.builder().id(Integer.toString(i)).build())
            .collect(Collectors.toList());
    DeleteMessageBatchResponse response =
        DeleteMessageBatchResponse.builder().successful(entries).build();

    SqsAsyncClient awsClient = mock(SqsAsyncClient.class);
    when(awsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(response));

    Source<Message, NotUsed> source = Source.fromIterator(messages::iterator);
    CompletionStage<List<SqsAckResultEntry>> stage =
        //  #batch-ack
        source
            .map(m -> MessageAction.delete(m))
            .via(SqsAckFlow.grouped(queueUrl, SqsAckGroupedSettings.create(), awsClient))
            .runWith(Sink.seq(), materializer);

    // #batch-ack

    List<SqsAckResultEntry> results = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) {
      SqsAckResultEntry r = results.get(i);
      Message m = messages.get(i);

      MessageAction messageAction = MessageAction.delete(m);
      DeleteMessageBatchResultEntry result = entries.get(i);

      assertEquals(messageAction, r.messageAction());
      assertEquals(result, r.result());
    }

    verify(awsClient, times(1)).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
  }

  @Test
  public void testBatchChangeMessageVisibility() throws Exception {
    final String queueUrl = "none";
    List<Message> messages = createMessages();
    List<ChangeMessageVisibilityBatchResultEntry> entries =
        IntStream.range(0, messages.size())
            .mapToObj(
                i ->
                    ChangeMessageVisibilityBatchResultEntry.builder()
                        .id(Integer.toString(i))
                        .build())
            .collect(Collectors.toList());
    ChangeMessageVisibilityBatchResponse response =
        ChangeMessageVisibilityBatchResponse.builder().successful(entries).build();

    SqsAsyncClient awsClient = mock(SqsAsyncClient.class);
    when(awsClient.changeMessageVisibilityBatch(any(ChangeMessageVisibilityBatchRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(response));

    Source<Message, NotUsed> source = Source.fromIterator(messages::iterator);
    CompletionStage<List<SqsAckResultEntry>> stage =
        // #batch-requeue
        source
            .map(m -> MessageAction.changeMessageVisibility(m, 5))
            .via(SqsAckFlow.grouped(queueUrl, SqsAckGroupedSettings.create(), awsClient))
            .runWith(Sink.seq(), materializer);
    // #batch-requeue

    List<SqsAckResultEntry> results = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) {
      SqsAckResultEntry r = results.get(i);
      Message m = messages.get(i);

      MessageAction messageAction = MessageAction.changeMessageVisibility(m, 5);
      ChangeMessageVisibilityBatchResultEntry result = entries.get(i);

      assertEquals(messageAction, r.messageAction());
      assertEquals(result, r.result());
    }

    verify(awsClient, times(1))
        .changeMessageVisibilityBatch(any(ChangeMessageVisibilityBatchRequest.class));
  }

  @Test
  public void testBatchIgnore() throws Exception {
    final String queueUrl = "none";
    List<Message> messages = createMessages();

    SqsAsyncClient awsClient = mock(SqsAsyncClient.class);

    Source<Message, NotUsed> source = Source.fromIterator(messages::iterator);
    CompletionStage<List<SqsAckResultEntry>> stage =
        // #batch-ignore
        source
            .map(m -> MessageAction.ignore(m))
            .via(SqsAckFlow.grouped(queueUrl, SqsAckGroupedSettings.create(), awsClient))
            .runWith(Sink.seq(), materializer);
    // #batch-ignore

    List<SqsAckResultEntry> results = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
    for (int i = 0; i < 10; i++) {
      SqsAckResultEntry r = results.get(i);
      Message m = messages.get(i);

      MessageAction messageAction = MessageAction.ignore(m);

      assertEquals(messageAction, r.messageAction());
      assertEquals(NotUsed.getInstance(), r.result());
    }
  }
}
