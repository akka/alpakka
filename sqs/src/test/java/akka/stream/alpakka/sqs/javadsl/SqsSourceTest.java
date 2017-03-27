/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.javadsl;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.sqs.SqsSourceSettings;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.JavaTestKit;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class SqsSourceTest extends BaseSqsTest {

    static ActorSystem system;
    static ActorMaterializer materializer;
    static SqsSourceSettings sqsSourceSettings;

    @BeforeClass
    public static void setup() {

        //#init-mat
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);
        //#init-mat

        sqsSourceSettings = SqsSourceSettings.create(20, 100, 10);
    }

    @AfterClass
    public static void teardown() {
        JavaTestKit.shutdownActorSystem(system);
    }

    String randomQueueUrl() {
        return sqsClient.createQueue(String.format("queue-%s", new Random().nextInt())).getQueueUrl();
    }

    @Test
    public void streamFromQueue() throws Exception {

        final String queueUrl = randomQueueUrl();
        sqsClient.sendMessage(queueUrl, "alpakka");

        //#run
        final CompletionStage<String> cs = SqsSource.create(queueUrl, sqsSourceSettings, sqsClient)
            .map(m -> m.getBody())
            .runWith(Sink.head(), materializer);
        //#run

        assertEquals("alpakka", cs.toCompletableFuture().get(10, TimeUnit.SECONDS));

    }

    @Test
    public void streamFromQueueWithCustomClient() throws Exception {

        final String queueUrl = randomQueueUrl();
        sqsClient.sendMessage(queueUrl, "alpakka");

        //#run
        final CompletionStage<String> cs = SqsSource.create(queueUrl, sqsSourceSettings, sqsClient)
            .map(m -> m.getBody())
            .take(1)
            .runWith(Sink.head(), materializer);
        //#run

        assertEquals("alpakka", cs.toCompletableFuture().get(10, TimeUnit.SECONDS));

    }
}
