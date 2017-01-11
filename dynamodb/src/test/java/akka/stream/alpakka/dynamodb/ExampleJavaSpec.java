/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.dynamodb;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.dynamodb.impl.DynamoSettings;
import akka.stream.alpakka.dynamodb.javadsl.DynamoClient;
import akka.testkit.JavaTestKit;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.DescribeLimitsRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeLimitsResult;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

public class ExampleJavaSpec {

    static ActorSystem system;
    static ActorMaterializer materializer;
    static DynamoSettings dynamoSettings;
    static DynamoClient dynamoClient;
    static DynamoDBProxyServer serverRunner;

    @BeforeClass
    public static void setup() throws Exception {

        //#init-client
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);
        //#init-client

        //#client-construct
        dynamoSettings = DynamoSettings.apply(system);
        dynamoClient = DynamoClient.create(dynamoSettings, system, materializer);
        //#client-construct

        String[] serverArgs = new String[] { "-port", Integer.toString(dynamoSettings.port()), "-inMemory" };
        serverRunner = ServerRunner.createServerFromCommandLineArgs(serverArgs);
        serverRunner.start();

    }

    @AfterClass
    public static void teardown() throws Exception {
        JavaTestKit.shutdownActorSystem(system);
        serverRunner.stop();
    }

    @Test
    public void listTables() throws Exception {
        //#simple-request
        final Future<DescribeLimitsResult> describeLimitsResultFuture = dynamoClient.describeLimits(new DescribeLimitsRequest());
        //#simple-request
        final Duration duration = Duration.create(5, "seconds");
        DescribeLimitsResult result = Await.result(describeLimitsResultFuture, duration);
    }

}
