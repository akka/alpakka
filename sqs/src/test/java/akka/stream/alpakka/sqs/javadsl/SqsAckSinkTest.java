/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.sqs.MessageAction;
import akka.stream.alpakka.sqs.scaladsl.AckResult;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.Sink;
import akka.testkit.javadsl.TestKit;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Option;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.junit.Assert.assertEquals;

public class SqsAckSinkTest extends BaseSqsTest {

    private static ActorSystem system;
    private static ActorMaterializer materializer;


    @BeforeClass
    public static void setup() {

        //#init-mat
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);
        //#init-mat
    }

    @AfterClass
    public static void teardown() {
        TestKit.shutdownActorSystem(system);
    }

    @Test
    public void testAcknowledge() throws Exception {
        final String queueUrl = "none";
        AmazonSQSAsync awsClient = mock(AmazonSQSAsync.class);
        when(awsClient.deleteMessageAsync(any(DeleteMessageRequest.class), any())).thenAnswer(
                invocation -> {
                    DeleteMessageRequest request = invocation.getArgument(0);
                    invocation
                            .<AsyncHandler< DeleteMessageRequest, DeleteMessageResult>>getArgument(1)
                            .onSuccess(request, new DeleteMessageResult());
                    return new CompletableFuture<DeleteMessageResult>();
                }
        );

        //#ack
        Tuple2<Message, MessageAction> pair = new Tuple2<>(
                new Message().withBody("test"),
                MessageAction.delete()
        );
        CompletionStage<Done> done = Source
                .single(pair)
                .runWith(SqsAckSink.create(queueUrl, awsClient), materializer);

        done.toCompletableFuture().get(1, TimeUnit.SECONDS);
        //#ack
        verify(awsClient).deleteMessageAsync(any(DeleteMessageRequest.class), any());
    }

    @Test
    public void testAcknowledgeViaFlow() throws Exception {
        final String queueUrl = "none";
        AmazonSQSAsync awsClient = mock(AmazonSQSAsync.class);
        when(awsClient.deleteMessageAsync(any(DeleteMessageRequest.class), any())).thenAnswer(
                invocation -> {
                    DeleteMessageRequest request = invocation.getArgument(0);
                    invocation
                            .<AsyncHandler< DeleteMessageRequest, DeleteMessageResult>>getArgument(1)
                            .onSuccess(request, new DeleteMessageResult());
                    return new CompletableFuture<DeleteMessageResult>();
                }
        );

        //#flow-ack
        Tuple2<Message, MessageAction> pair = new Tuple2<>(
                new Message().withBody("test-ack-flow"),
                MessageAction.delete()
        );
        CompletionStage<Done> done = Source
                .single(pair)
                .via(SqsAckFlow.create(queueUrl, awsClient))
                .runWith(Sink.ignore(), materializer);

        done.toCompletableFuture().get(1, TimeUnit.SECONDS);
        //#flow-ack
        verify(awsClient).deleteMessageAsync(any(DeleteMessageRequest.class), any());
    }

    @Test
    public void testChangeMessageVisibility() throws Exception {
        final String queueUrl = "none";
        AmazonSQSAsync awsClient = mock(AmazonSQSAsync.class);
        when(awsClient.changeMessageVisibilityAsync(any(ChangeMessageVisibilityRequest.class), any())).thenAnswer(
                invocation -> {
                    ChangeMessageVisibilityRequest request = invocation.getArgument(0);
                    invocation
                            .<AsyncHandler<ChangeMessageVisibilityRequest, ChangeMessageVisibilityResult>>getArgument(1)
                            .onSuccess(request, new ChangeMessageVisibilityResult());
                    return new CompletableFuture<ChangeMessageVisibilityResult>();
                }
        );

        //#requeue
        Tuple2<Message, MessageAction> pair = new Tuple2<>(
                new Message().withBody("test"),
                MessageAction.changeMessageVisibility(12)
        );
        CompletionStage<Done> done = Source
                .single(pair)
                .runWith(SqsAckSink.create(queueUrl, awsClient), materializer);
        done.toCompletableFuture().get(1, TimeUnit.SECONDS);
        //#requeue

        verify(awsClient)
                .changeMessageVisibilityAsync(
                        any(ChangeMessageVisibilityRequest.class),
                        any()
                );
    }

    @Test
    public void testIgnore() throws Exception {
        final String queueUrl = "none";
        AmazonSQSAsync awsClient = mock(AmazonSQSAsync.class);

        //#ignore
        Tuple2<Message, MessageAction> pair = new Tuple2<>(
                new Message().withBody("test"),
                MessageAction.ignore()
        );
        CompletionStage<AckResult> stage = Source
                .single(pair)
                .via(SqsAckFlow.create(queueUrl, awsClient))
                .runWith(Sink.head(), materializer);
        AckResult result = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
        //#ignore

        assertEquals(Option.empty(), result.metadata());
        assertEquals("test", result.message());
    }

    @Test
    public void testBatchAcknowledge() throws Exception {
        final String queueUrl = "none";
        AmazonSQSAsync awsClient = mock(AmazonSQSAsync.class);
        when(awsClient.deleteMessageBatchAsync(any(DeleteMessageBatchRequest.class), any())).thenAnswer(
                invocation -> {
                    DeleteMessageBatchRequest request = invocation.getArgument(0);
                    invocation
                            .<AsyncHandler< DeleteMessageBatchRequest, DeleteMessageBatchResult>>getArgument(1)
                            .onSuccess(request, new DeleteMessageBatchResult());
                    return new CompletableFuture<DeleteMessageBatchRequest>();
                }
        );

        //#batch-ack
        List<Tuple2<Message, MessageAction>> messages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            messages.add(new Tuple2<>(
                    new Message().withBody("test"),
                    MessageAction.delete()
            ));
        }
        CompletionStage<Done> done = Source
                .fromIterator(() -> messages.iterator())
                .via(SqsAckFlow.grouped(queueUrl, awsClient))
                .runWith(Sink.ignore(), materializer);

        done.toCompletableFuture().get(1, TimeUnit.SECONDS);
        //#batch-ack
        verify(awsClient).deleteMessageBatchAsync(any(DeleteMessageBatchRequest.class), any());
    }

    @Test
    public void testBatchChangeMessageVisibility() throws Exception {
        final String queueUrl = "none";
        AmazonSQSAsync awsClient = mock(AmazonSQSAsync.class);
        when(awsClient.changeMessageVisibilityBatchAsync(any(ChangeMessageVisibilityBatchRequest.class), any())).thenAnswer(
                invocation -> {
                    ChangeMessageVisibilityBatchRequest request = invocation.getArgument(0);
                    invocation
                            .<AsyncHandler< ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityBatchResult>>getArgument(1)
                            .onSuccess(request, new ChangeMessageVisibilityBatchResult());
                    return new CompletableFuture<ChangeMessageVisibilityBatchRequest>();
                }
        );

        //#batch-requeue
        List<Tuple2<Message, MessageAction>> messages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            messages.add(new Tuple2<>(
                    new Message().withBody("test"),
                    MessageAction.changeMessageVisibility(5)
            ));
        }
        CompletionStage<Done> done = Source
                .fromIterator(() -> messages.iterator())
                .via(SqsAckFlow.grouped(queueUrl, awsClient))
                .runWith(Sink.ignore(), materializer);

        done.toCompletableFuture().get(1, TimeUnit.SECONDS);
        //#batch-requeue
        verify(awsClient).changeMessageVisibilityBatchAsync(any(ChangeMessageVisibilityBatchRequest.class), any());
    }

    @Test
    public void testBatchIgnore() throws Exception {
        final String queueUrl = "none";
        AmazonSQSAsync awsClient = mock(AmazonSQSAsync.class);

        //#batch-ignore
        List<Tuple2<Message, MessageAction>> messages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            messages.add(new Tuple2<>(
                    new Message().withBody("test"),
                    MessageAction.ignore()
            ));
        }
        CompletionStage<List<AckResult>> stage = Source
                .fromIterator(() -> messages.iterator())
                .via(SqsAckFlow.grouped(queueUrl, awsClient))
                .runWith(Sink.seq(), materializer);
        List<AckResult> result = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
        //#batch-ignore

        assertEquals(10, result.size());
        for (int i = 0; i< 10 ; i++) {
            assertEquals(Option.empty(), result.get(i).metadata());
            assertEquals("test", result.get(i).message());
        }
    }
}
