/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.dynamodb;

import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.dynamodb.impl.DynamoSettings;
import akka.stream.alpakka.dynamodb.javadsl.DynamoClient;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

public class ExampleTest {

    static ActorSystem system;
    static ActorMaterializer materializer;
    static DynamoSettings settings;
    static DynamoClient client;

    public static Pair<ActorSystem, ActorMaterializer> setupMaterializer() {
        //#init-client
        final ActorSystem system = ActorSystem.create();
        final ActorMaterializer materializer = ActorMaterializer.create(system);
        //#init-client
        return Pair.create(system, materializer);
    }

    public static Pair<DynamoSettings, DynamoClient> setupClient() {
        //#client-construct
        final DynamoSettings settings = DynamoSettings.apply(system);
        final DynamoClient client = DynamoClient.create(settings, system, materializer);
        //#client-construct
        return Pair.create(settings, client);
    }

    @BeforeClass
    public static void setup() throws Exception {
        System.setProperty("aws.accessKeyId", "someKeyId");
        System.setProperty("aws.secretKey", "someSecretKey");

        final Pair<ActorSystem, ActorMaterializer> sysmat = setupMaterializer();
        system = sysmat.first();
        materializer = sysmat.second();

        final Pair<DynamoSettings, DynamoClient> setclient = setupClient();
        settings = setclient.first();
        client = setclient.second();
    }

    @Test
    public void listTables() throws Exception {
        //#simple-request
        final Future<ListTablesResult> listTablesResultFuture = client.listTables(new ListTablesRequest());
        //#simple-request
        final Duration duration = Duration.create(5, "seconds");
        ListTablesResult result = Await.result(listTablesResultFuture, duration);
    }

}
