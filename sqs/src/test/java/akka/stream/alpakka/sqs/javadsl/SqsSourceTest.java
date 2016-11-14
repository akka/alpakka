/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.javadsl;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.testkit.JavaTestKit;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
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
    static SQSRestServer sqsServer;
    static AmazonSQSAsyncClient sqsClient;


    @BeforeClass
    public static void setup() {

        //#init-mat
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);
        //#init-mat

        sqsServer = SQSRestServerBuilder
            .withActorSystem(system)
            .withInterface("localhost")
            .withPort(9325)
            .start();

        sqsServer.waitUntilStarted();

        //#init-client
        sqsClient = new AmazonSQSAsyncClient()
            .withEndpoint("http://localhost:9325");
        //#init-client

    }

    @AfterClass
    public static void teardown() {
        sqsServer.stopAndWait();
        JavaTestKit.shutdownActorSystem(system);
    }

    static String randomQueueUrl() {
        //there is a bug in elasticmq which returns incorrect queueUrls
        return sqsClient.createQueue(String.format("queue-%s", new Random().nextInt())).getQueueUrl().replaceFirst("9324","9325");
    }

    @Test
    public void streamFromQueue() throws Exception {

        final String queueUrl = randomQueueUrl();

        final List<String> input = IntStream.range(1, 100).boxed().map(i -> String.format("alpakka-%s", i)).collect(Collectors.toList());
        input.forEach(m -> sqsClient.sendMessage(queueUrl, m));

        //#run
        final CompletionStage<List<String>> cs = SqsSource.create(queueUrl, sqsClient)
            .map(m -> m.getBody())
            .takeWithin(Duration.create(1, TimeUnit.SECONDS))
            .runWith(Sink.seq(), materializer);
        //#run

        assertEquals(input.size(), cs.toCompletableFuture().get(3, TimeUnit.SECONDS).size());

    }
}
