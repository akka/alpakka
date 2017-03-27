/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.sqs.Ack;
import akka.stream.alpakka.sqs.MessageAction;
import akka.stream.alpakka.sqs.RequeueWithDelay;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.Sink;
import akka.testkit.JavaTestKit;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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
                new Ack()
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
                new Ack()
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
    public void testRequeueWithDelay() throws Exception {
        final String queueUrl = "none";
        AmazonSQSAsync awsClient = mock(AmazonSQSAsync.class);
        when(awsClient.sendMessageAsync(any(SendMessageRequest.class), any())).thenAnswer(
                invocation -> {
                    SendMessageRequest request = invocation.getArgument(0);
                    invocation
                            .<AsyncHandler<SendMessageRequest, SendMessageResult>>getArgument(1)
                            .onSuccess(request, new SendMessageResult());
                    return new CompletableFuture<SendMessageResult>();
                }
        );

        //#requeue
        Tuple2<Message, MessageAction> pair = new Tuple2<>(
                new Message().withBody("test"),
                new RequeueWithDelay(12)
        );
        CompletionStage<Done> done = Source
                .single(pair)
                .runWith(SqsAckSink.create(queueUrl, awsClient), materializer);
        done.toCompletableFuture().get(1, TimeUnit.SECONDS);
        //#requeue

        verify(awsClient)
                .sendMessageAsync(
                        any(SendMessageRequest.class),
                        any()
                );
    }
}
