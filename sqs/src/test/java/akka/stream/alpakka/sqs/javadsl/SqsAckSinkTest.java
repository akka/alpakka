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
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

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
        AmazonSQSAsync awsClient = spy(AmazonSQSAsync.class);

        //#run
        Tuple2<Message, MessageAction> pair = new Tuple2<>(
                new Message().withBody("test"),
                new Ack()
        );
        Future<Done> done = Source
                .single(pair)
                .runWith(SqsAckSink.create(queueUrl, awsClient), materializer);
        //#run

        verify(awsClient).deleteMessageAsync(any(DeleteMessageRequest.class), any());
    }

    @Test
    public void testRequeueWithDelay() throws Exception {
        final String queueUrl = "none";
        AmazonSQSAsync awsClient = spy(AmazonSQSAsync.class);

        //#run
        Tuple2<Message, MessageAction> pair = new Tuple2<>(
                new Message().withBody("test"),
                new RequeueWithDelay(12)
        );
        Future<Done> done = Source
                .single(pair)
                .runWith(SqsAckSink.create(queueUrl, awsClient), materializer);
        //#run

        verify(awsClient)
                .sendMessageAsync(
                        any(SendMessageRequest.class),
                        any()
                );
    }
}
