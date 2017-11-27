/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.sqs.MessageAction;
import akka.stream.alpakka.sqs.scaladsl.AckResult;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.Sink;
import akka.testkit.JavaTestKit;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Option;
import scala.Tuple2;
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
        JavaTestKit.shutdownActorSystem(system);
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
}
