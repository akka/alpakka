/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
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
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.*;
import org.junit.Test;
import scala.Option;

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

      messages.add(new Message().withBody("test-" + i));
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
    AmazonSQSAsync awsClient = mock(AmazonSQSAsync.class);
    when(awsClient.deleteMessageAsync(any(DeleteMessageRequest.class), any()))
        .thenAnswer(
            invocation -> {
              DeleteMessageRequest request = invocation.getArgument(0);
              invocation
                  .<AsyncHandler<DeleteMessageRequest, DeleteMessageResult>>getArgument(1)
                  .onSuccess(request, new DeleteMessageResult());
              return new CompletableFuture<DeleteMessageResult>();
            });

    List<Message> messages = createMessages();
    Source<Message, NotUsed> source = Source.fromIterator(() -> messages.iterator());

    CompletionStage<Done> done =
        // #ack
        source
            .map(m -> MessageAction.delete(m))
            .runWith(SqsAckSink.create(queueUrl, SqsAckSettings.create(), awsClient), materializer);
    // #ack

    done.toCompletableFuture().get(1, TimeUnit.SECONDS);
    verify(awsClient).deleteMessageAsync(any(DeleteMessageRequest.class), any());
  }

  @Test
  public void testAcknowledgeViaFlow() throws Exception {
    final String queueUrl = "none";
    AmazonSQSAsync awsClient = mock(AmazonSQSAsync.class);
    when(awsClient.deleteMessageAsync(any(DeleteMessageRequest.class), any()))
        .thenAnswer(
            invocation -> {
              DeleteMessageRequest request = invocation.getArgument(0);
              invocation
                  .<AsyncHandler<DeleteMessageRequest, DeleteMessageResult>>getArgument(1)
                  .onSuccess(request, new DeleteMessageResult());
              return new CompletableFuture<DeleteMessageResult>();
            });

    List<Message> messages = createMessages();
    Source<Message, NotUsed> source = Source.fromIterator(() -> messages.iterator());

    CompletionStage<List<SqsAckResult>> stage =
        // #flow-ack
        source
            .map(m -> MessageAction.delete(m))
            .via(SqsAckFlow.create(queueUrl, SqsAckSettings.create(), awsClient))
            .runWith(Sink.seq(), materializer);
    // #flow-ack

    List<SqsAckResult> result = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertEquals(10, result.size());
    for (int i = 0; i < 10; i++) {
      SqsAckResult r = result.get(i);
      Message m = messages.get(i);
      assertEquals(MessageAction.delete(m), r.messageAction());
    }

    verify(awsClient).deleteMessageAsync(any(DeleteMessageRequest.class), any());
  }

  @Test
  public void testChangeMessageVisibility() throws Exception {
    final String queueUrl = "none";
    AmazonSQSAsync awsClient = mock(AmazonSQSAsync.class);
    when(awsClient.changeMessageVisibilityAsync(any(ChangeMessageVisibilityRequest.class), any()))
        .thenAnswer(
            invocation -> {
              ChangeMessageVisibilityRequest request = invocation.getArgument(0);
              invocation
                  .<AsyncHandler<ChangeMessageVisibilityRequest, ChangeMessageVisibilityResult>>
                      getArgument(1)
                  .onSuccess(request, new ChangeMessageVisibilityResult());
              return new CompletableFuture<ChangeMessageVisibilityResult>();
            });

    List<Message> messages = createMessages();
    Source<Message, NotUsed> source = Source.fromIterator(() -> messages.iterator());

    CompletionStage<Done> done =
        // #requeue
        source
            .map(m -> MessageAction.changeMessageVisibility(m, 12))
            .runWith(SqsAckSink.create(queueUrl, SqsAckSettings.create(), awsClient), materializer);
    // #requeue
    done.toCompletableFuture().get(1, TimeUnit.SECONDS);

    verify(awsClient)
        .changeMessageVisibilityAsync(any(ChangeMessageVisibilityRequest.class), any());
  }

  @Test
  public void testIgnore() throws Exception {
    final String queueUrl = "none";
    AmazonSQSAsync awsClient = mock(AmazonSQSAsync.class);

    List<Message> messages = createMessages();
    Source<Message, NotUsed> source = Source.fromIterator(() -> messages.iterator());

    CompletionStage<List<SqsAckResult>> stage =
        // #ignore
        source
            .map(m -> MessageAction.ignore(m))
            .via(SqsAckFlow.create(queueUrl, SqsAckSettings.create(), awsClient))
            .runWith(Sink.seq(), materializer);
    // #ignore

    List<SqsAckResult> result = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
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
    final DeleteMessageBatchResult metadata = new DeleteMessageBatchResult();
    AmazonSQSAsync awsClient = mock(AmazonSQSAsync.class);
    when(awsClient.deleteMessageBatchAsync(any(DeleteMessageBatchRequest.class), any()))
        .thenAnswer(
            invocation -> {
              DeleteMessageBatchRequest request = invocation.getArgument(0);
              invocation
                  .<AsyncHandler<DeleteMessageBatchRequest, DeleteMessageBatchResult>>getArgument(1)
                  .onSuccess(request, metadata);
              return new CompletableFuture<DeleteMessageBatchRequest>();
            });

    List<Message> messages = createMessages();
    Source<Message, NotUsed> source = Source.fromIterator(() -> messages.iterator());

    CompletionStage<List<SqsAckResult>> stage =
        //  #batch-ack
        source
            .map(m -> MessageAction.delete(m))
            .via(SqsAckFlow.grouped(queueUrl, SqsAckGroupedSettings.create(), awsClient))
            .runWith(Sink.seq(), materializer);

    // #batch-ack

    List<SqsAckResult> result = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertEquals(10, result.size());
    for (int i = 0; i < 10; i++) {
      SqsAckResult r = result.get(i);
      Message m = messages.get(i);

      assertEquals(Option.apply(metadata), r.metadata());
      assertEquals(MessageAction.delete(m), r.messageAction());
    }

    verify(awsClient).deleteMessageBatchAsync(any(DeleteMessageBatchRequest.class), any());
  }

  @Test
  public void testBatchChangeMessageVisibility() throws Exception {
    final String queueUrl = "none";
    final ChangeMessageVisibilityBatchResult metadata = new ChangeMessageVisibilityBatchResult();
    AmazonSQSAsync awsClient = mock(AmazonSQSAsync.class);
    when(awsClient.changeMessageVisibilityBatchAsync(
            any(ChangeMessageVisibilityBatchRequest.class), any()))
        .thenAnswer(
            invocation -> {
              ChangeMessageVisibilityBatchRequest request = invocation.getArgument(0);
              invocation
                  .<AsyncHandler<
                          ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityBatchResult>>
                      getArgument(1)
                  .onSuccess(request, metadata);
              return new CompletableFuture<ChangeMessageVisibilityBatchRequest>();
            });

    List<Message> messages = createMessages();
    Source<Message, NotUsed> source = Source.fromIterator(() -> messages.iterator());

    CompletionStage<List<SqsAckResult>> stage =
        // #batch-requeue
        source
            .map(m -> MessageAction.changeMessageVisibility(m, 5))
            .via(SqsAckFlow.grouped(queueUrl, SqsAckGroupedSettings.create(), awsClient))
            .runWith(Sink.seq(), materializer);
    // #batch-requeue

    List<SqsAckResult> result = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertEquals(10, result.size());
    for (int i = 0; i < 10; i++) {
      SqsAckResult r = result.get(i);
      Message m = messages.get(i);

      assertEquals(Option.apply(metadata), r.metadata());
      assertEquals(MessageAction.changeMessageVisibility(m, 5), r.messageAction());
    }

    verify(awsClient)
        .changeMessageVisibilityBatchAsync(any(ChangeMessageVisibilityBatchRequest.class), any());
  }

  @Test
  public void testBatchIgnore() throws Exception {
    final String queueUrl = "none";
    AmazonSQSAsync awsClient = mock(AmazonSQSAsync.class);

    List<Message> messages = createMessages();
    Source<Message, NotUsed> source = Source.fromIterator(() -> messages.iterator());

    CompletionStage<List<SqsAckResult>> stage =
        // #batch-ignore
        source
            .map(m -> MessageAction.ignore(m))
            .via(SqsAckFlow.grouped(queueUrl, SqsAckGroupedSettings.create(), awsClient))
            .runWith(Sink.seq(), materializer);
    // #batch-ignore

    List<SqsAckResult> result = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertEquals(10, result.size());
    for (int i = 0; i < 10; i++) {
      SqsAckResult r = result.get(i);
      Message m = messages.get(i);

      assertEquals(Option.empty(), r.metadata());
      assertEquals(MessageAction.ignore(m), r.messageAction());
    }
  }
}
