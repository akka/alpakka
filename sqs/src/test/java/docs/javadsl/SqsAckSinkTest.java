/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.stream.alpakka.sqs.MessageAction;
import akka.stream.alpakka.sqs.SqsAckResult;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.junit.Assert.assertEquals;

public class SqsAckSinkTest extends BaseSqsTest {

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

    // #ack
    MessageAction action = MessageAction.delete(new Message().withBody("test"));
    CompletionStage<Done> done =
        Source.single(action).runWith(SqsAckSink.create(queueUrl, awsClient), materializer);
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

    // #flow-ack
    MessageAction action = MessageAction.delete(new Message().withBody("test-ack-flow"));
    CompletionStage<Done> done =
        Source.single(action)
            .via(SqsAckFlow.create(queueUrl, awsClient))
            .runWith(Sink.ignore(), materializer);
    // #flow-ack

    done.toCompletableFuture().get(1, TimeUnit.SECONDS);
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

    // #requeue
    MessageAction action =
        MessageAction.changeMessageVisibility(new Message().withBody("test"), 12);
    CompletionStage<Done> done =
        Source.single(action).runWith(SqsAckSink.create(queueUrl, awsClient), materializer);
    // #requeue
    done.toCompletableFuture().get(1, TimeUnit.SECONDS);

    verify(awsClient)
        .changeMessageVisibilityAsync(any(ChangeMessageVisibilityRequest.class), any());
  }

  @Test
  public void testIgnore() throws Exception {
    final String queueUrl = "none";
    AmazonSQSAsync awsClient = mock(AmazonSQSAsync.class);

    // #ignore
    MessageAction action = MessageAction.ignore(new Message().withBody("test"));
    CompletionStage<SqsAckResult> stage =
        Source.single(action)
            .via(SqsAckFlow.create(queueUrl, awsClient))
            .runWith(Sink.head(), materializer);
    // #ignore
    SqsAckResult result = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);

    assertEquals(Option.empty(), result.metadata());
    assertEquals("test", result.message());
  }

  @Test
  public void testBatchAcknowledge() throws Exception {
    final String queueUrl = "none";
    AmazonSQSAsync awsClient = mock(AmazonSQSAsync.class);
    when(awsClient.deleteMessageBatchAsync(any(DeleteMessageBatchRequest.class), any()))
        .thenAnswer(
            invocation -> {
              DeleteMessageBatchRequest request = invocation.getArgument(0);
              invocation
                  .<AsyncHandler<DeleteMessageBatchRequest, DeleteMessageBatchResult>>getArgument(1)
                  .onSuccess(request, new DeleteMessageBatchResult());
              return new CompletableFuture<DeleteMessageBatchRequest>();
            });

    // #batch-ack
    List<MessageAction> messages = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      messages.add(MessageAction.delete(new Message().withBody("test")));
    }
    CompletionStage<Done> done =
        Source.fromIterator(() -> messages.iterator())
            .via(SqsAckFlow.grouped(queueUrl, awsClient))
            .runWith(Sink.ignore(), materializer);
    // #batch-ack

    done.toCompletableFuture().get(1, TimeUnit.SECONDS);
    verify(awsClient).deleteMessageBatchAsync(any(DeleteMessageBatchRequest.class), any());
  }

  @Test
  public void testBatchChangeMessageVisibility() throws Exception {
    final String queueUrl = "none";
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
                  .onSuccess(request, new ChangeMessageVisibilityBatchResult());
              return new CompletableFuture<ChangeMessageVisibilityBatchRequest>();
            });

    // #batch-requeue
    List<MessageAction> messages = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      messages.add(MessageAction.changeMessageVisibility(new Message().withBody("test"), 5));
    }
    CompletionStage<Done> done =
        Source.fromIterator(() -> messages.iterator())
            .via(SqsAckFlow.grouped(queueUrl, awsClient))
            .runWith(Sink.ignore(), materializer);
    // #batch-requeue

    done.toCompletableFuture().get(1, TimeUnit.SECONDS);
    verify(awsClient)
        .changeMessageVisibilityBatchAsync(any(ChangeMessageVisibilityBatchRequest.class), any());
  }

  @Test
  public void testBatchIgnore() throws Exception {
    final String queueUrl = "none";
    AmazonSQSAsync awsClient = mock(AmazonSQSAsync.class);

    // #batch-ignore
    List<MessageAction> messages = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      messages.add(MessageAction.ignore(new Message().withBody("test")));
    }
    CompletionStage<List<SqsAckResult>> stage =
        Source.fromIterator(() -> messages.iterator())
            .via(SqsAckFlow.grouped(queueUrl, awsClient))
            .runWith(Sink.seq(), materializer);
    // #batch-ignore
    List<SqsAckResult> result = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);

    assertEquals(10, result.size());
    for (int i = 0; i < 10; i++) {
      assertEquals(Option.empty(), result.get(i).metadata());
      assertEquals("test", result.get(i).message());
    }
  }
}
