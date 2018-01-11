/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.javadsl;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.sqs.SqsSourceSettings;
import akka.stream.javadsl.Sink;
import akka.testkit.javadsl.TestKit;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class SqsSourceTest extends BaseSqsTest {

    private static ActorSystem system;
    private static ActorMaterializer materializer;
    private static SqsSourceSettings sqsSourceSettings;

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
        TestKit.shutdownActorSystem(system);
    }

    private String randomQueueUrl() {
        return sqsClient.createQueue(String.format("queue-%s", new Random().nextInt())).getQueueUrl();
    }

    @Test
    public void streamFromQueue() throws Exception {

        final String queueUrl = randomQueueUrl();
        sqsClient.sendMessage(queueUrl, "alpakka");

        //#run
        final CompletionStage<String> cs = SqsSource.create(queueUrl, sqsSourceSettings, sqsClient)
            .map(Message::getBody)
            .runWith(Sink.head(), materializer);
        //#run

        assertEquals("alpakka", cs.toCompletableFuture().get(10, TimeUnit.SECONDS));

    }

    @Test
    public void streamFromQueueWithCustomClient() throws Exception {

        final String queueUrl = randomQueueUrl();

        //#init-custom-client
        AmazonSQSAsync customSqsClient =
          AmazonSQSAsyncClientBuilder
            .standard()
            .withCredentials(credentialsProvider)
            .withExecutorFactory(() -> Executors.newFixedThreadPool(10))
            .withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(sqsEndpoint, "eu-central-1"))
            .build();
        //#init-custom-client

        sqsClient.sendMessage(queueUrl, "alpakka");

        //#run
        final CompletionStage<String> cs = SqsSource.create(queueUrl, sqsSourceSettings, customSqsClient)
                .map(Message::getBody)
                .take(1)
                .runWith(Sink.head(), materializer);
        //#run

        assertEquals("alpakka", cs.toCompletableFuture().get(10, TimeUnit.SECONDS));

    }
}
