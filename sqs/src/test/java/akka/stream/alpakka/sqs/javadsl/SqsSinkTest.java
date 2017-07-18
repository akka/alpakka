/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.JavaTestKit;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class SqsSinkTest extends BaseSqsTest {

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

    private String randomQueueUrl() {
        return sqsClient.createQueue(String.format("queue-%s", new Random().nextInt())).getQueueUrl();
    }

    @Test
    public void sendToQueue() throws Exception {

        final String queueUrl = randomQueueUrl();

        //#run
        CompletionStage<Done> done = Source
          .single("alpakka")
          .runWith(SqsSink.create(queueUrl, sqsClient), materializer);

        done.toCompletableFuture().get(1, TimeUnit.SECONDS);
        //#run
        List<Message> messages = sqsClient.receiveMessage(queueUrl).getMessages();

        assertEquals(1, messages.size());
        assertEquals("alpakka", messages.get(0).getBody());
    }

    @Test
    public void sendViaFlow() throws Exception {
        final String queueUrl = randomQueueUrl();

        //#flow
        CompletionStage<Done> done = Source
                .single("alpakka-flow")
                .via(SqsFlow.create(queueUrl, sqsClient))
                .runWith(Sink.ignore(), materializer);

        done.toCompletableFuture().get(1, TimeUnit.SECONDS);
        //#flow
        List<Message> messages = sqsClient.receiveMessage(queueUrl).getMessages();

        assertEquals(1, messages.size());
        assertEquals("alpakka-flow", messages.get(0).getBody());
    }

    @Test
    public void sendToQueueWithBatches() throws Exception {
        final String queueUrl = randomQueueUrl();

        //#group
        ArrayList<String> messagesToSend = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            messagesToSend.add("message - " + i);
        }

        CompletionStage<Done> done = Source
                .from(messagesToSend)
                .runWith(SqsSink.grouped(queueUrl,sqsClient), materializer);

        done.toCompletableFuture().get(1, TimeUnit.SECONDS);
        //#group

        List<Message> messagesFirstBatch = sqsClient.receiveMessage(new ReceiveMessageRequest().withQueueUrl(queueUrl).withMaxNumberOfMessages(10)).getMessages();
        List<Message> messagesSecondBatch = sqsClient.receiveMessage(new ReceiveMessageRequest().withQueueUrl(queueUrl).withMaxNumberOfMessages(10)).getMessages();

        assertEquals(20, messagesFirstBatch.size() + messagesSecondBatch.size());
    }

    @Test
    public void sendBatchesToQueue() throws Exception {
        final String queueUrl = randomQueueUrl();

        //#batch
        ArrayList<String> messagesToSend = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            messagesToSend.add("Message - " + i);
        }
        Iterable<String> it = messagesToSend;

        CompletionStage<Done> done = Source
                .single(it)
                .runWith(SqsSink.batch(queueUrl,sqsClient), materializer);

        done.toCompletableFuture().get(1, TimeUnit.SECONDS);
        //#batch

        List<Message> messagesFirstBatch = sqsClient.receiveMessage(new ReceiveMessageRequest().withQueueUrl(queueUrl).withMaxNumberOfMessages(10)).getMessages();

        assertEquals(10, messagesFirstBatch.size());
    }

    @Test
    public void sendMessageWithBatchesAsFlow() throws Exception {
        final String queueUrl = randomQueueUrl();

        ArrayList<String> messagesToSend = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            messagesToSend.add("Message - " + i);
        }

        CompletionStage<Done> done = Source
                .from(messagesToSend)
                .via(SqsFlow.grouped(queueUrl,sqsClient))
                .runWith(Sink.ignore(), materializer);

        done.toCompletableFuture().get(1, TimeUnit.SECONDS);

        List<Message> messagesFirstBatch = sqsClient.receiveMessage(new ReceiveMessageRequest().withQueueUrl(queueUrl).withMaxNumberOfMessages(10)).getMessages();

        assertEquals(10, messagesFirstBatch.size());
    }

    @Test
    public void sendBatchesAsFlow() throws Exception {
        final String queueUrl = randomQueueUrl();

        ArrayList<String> messagesToSend = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            messagesToSend.add("Message - " + i);
        }
        Iterable<String> it = messagesToSend;

        CompletionStage<Done> done = Source
                .single(it)
                .via(SqsFlow.batch(queueUrl,sqsClient))
                .runWith(Sink.ignore(), materializer);

        done.toCompletableFuture().get(1, TimeUnit.SECONDS);

        List<Message> messagesFirstBatch = sqsClient.receiveMessage(new ReceiveMessageRequest().withQueueUrl(queueUrl).withMaxNumberOfMessages(10)).getMessages();

        assertEquals(10, messagesFirstBatch.size());
    }

    @Test
    public void ackViaFlow() throws Exception {
        final String queueUrl = randomQueueUrl();

        //#flow
        CompletionStage<Done> done = Source
                .single("alpakka-flow")
                .via(SqsFlow.create(queueUrl, sqsClient))
                .runWith(Sink.ignore(), materializer);

        done.toCompletableFuture().get(1, TimeUnit.SECONDS);
        //#flow
        List<Message> messages = sqsClient.receiveMessage(queueUrl).getMessages();

        assertEquals(1, messages.size());
        assertEquals("alpakka-flow", messages.get(0).getBody());
    }
}
