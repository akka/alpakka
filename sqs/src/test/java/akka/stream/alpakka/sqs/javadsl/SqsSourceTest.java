/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.javadsl;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.testkit.JavaTestKit;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.duration.Duration;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class SqsSourceTest {

    static ActorSystem system;
    static ActorMaterializer materializer;
    static AWSCredentials credentials;
    static AmazonSQSAsyncClient sqsClient;


    @BeforeClass
    public static void setup() {

        //#init-mat
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);
        //#init-mat

        //#init-client
        credentials = new BasicAWSCredentials("x", "x");
        sqsClient = new AmazonSQSAsyncClient(credentials)
            .withEndpoint("http://localhost:9324");
        //#init-client

    }

    @AfterClass
    public static void teardown() {
        JavaTestKit.shutdownActorSystem(system);
    }

    static String randomQueueUrl() {
        return sqsClient.createQueue(String.format("queue-%s", new Random().nextInt())).getQueueUrl();
    }

    @Test
    public void streamFromQueue() throws Exception {

        final String queueUrl = randomQueueUrl();

        final List<String> input = IntStream.range(0, 100).boxed().map(i -> String.format("alpakka-%s", i)).collect(Collectors.toList());
        input.forEach(m -> sqsClient.sendMessage(queueUrl, m));

        //#run
        final CompletionStage<List<String>> cs = SqsSource.create(queueUrl, sqsClient)
            .map(m -> m.getBody())
            .take(100)
            .runWith(Sink.seq(), materializer);
        //#run

        assertEquals(input.size(), cs.toCompletableFuture().get(3, TimeUnit.SECONDS).size());

    }
}
