/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.sqs.Ack;
import akka.stream.alpakka.sqs.MessageAction;
import akka.stream.alpakka.sqs.RequeueWithDelay;
import akka.stream.javadsl.Source;
import akka.testkit.JavaTestKit;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.model.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class SqsAckSinkTest {

    private static ActorSystem system;
    private static ActorMaterializer materializer;


    @BeforeClass
    public static void setup() {

        //#init-mat
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);
        //#init-mat

        //#init-client
        AWSCredentials credentials = new BasicAWSCredentials("x", "x");
        AmazonSQSAsyncClient sqsClient = new AmazonSQSAsyncClient(credentials).withEndpoint("http://localhost:9324");
        //#init-client
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

        //#run
        Tuple2<Message, MessageAction> pair = new Tuple2<>(
                new Message().withBody("test"),
                new Ack()
        );
        Future<Done> done = Source
                .single(pair)
                .runWith(SqsAckSink.create(queueUrl, awsClient), materializer);
        Await.ready(done, new FiniteDuration(1, TimeUnit.SECONDS));
        //#run

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

        //#run
        Tuple2<Message, MessageAction> pair = new Tuple2<>(
                new Message().withBody("test"),
                new RequeueWithDelay(12)
        );
        Future<Done> done = Source
                .single(pair)
                .runWith(SqsAckSink.create(queueUrl, awsClient), materializer);
        Await.ready(done, new FiniteDuration(1, TimeUnit.SECONDS));
        //#run

        verify(awsClient)
                .sendMessageAsync(
                        any(SendMessageRequest.class),
                        any()
                );
    }
}
