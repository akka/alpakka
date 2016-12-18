/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class SqsSinkTest {

    private static ActorSystem system;
    private static ActorMaterializer materializer;
    private static AmazonSQSAsyncClient sqsClient;


    @BeforeClass
    public static void setup() {

        //#init-mat
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);
        //#init-mat

        //#init-client
        AWSCredentials credentials = new BasicAWSCredentials("x", "x");
        sqsClient = new AmazonSQSAsyncClient(credentials).withEndpoint("http://localhost:9324");
        //#init-client

    }

    @AfterClass
    public static void teardown() {
        JavaTestKit.shutdownActorSystem(system);
    }

    private static String randomQueueUrl() {
        return sqsClient.createQueue(String.format("queue-%s", new Random().nextInt())).getQueueUrl();
    }

    @Test
    public void sendToQueue() throws Exception {

        final String queueUrl = randomQueueUrl();

        //#run
        Future<Done> done = Source
                .single("alpakka")
                .runWith(SqsSink.create(queueUrl, sqsClient), materializer);
        Await.ready(done, new FiniteDuration(1, TimeUnit.SECONDS));
        //#run
        List<Message> messages = sqsClient.receiveMessage(queueUrl).getMessages();

        assertEquals(1, messages.size());
        assertEquals("alpakka", messages.get(0).getBody());
    }
}
