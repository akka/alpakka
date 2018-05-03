/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kudu.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.kudu.KuduTableSettings;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;


import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

@Ignore
public class KuduStageTest {
    private static ActorSystem system;
    private static Materializer materializer;
    private static Schema schema;
    private static String tableName;
    private static CreateTableOptions createTableOptions;

    private static KuduClient setupKuduClient() {
        final KuduClient kuduClient = new KuduClient.KuduClientBuilder("127.0.0.1:7051").build();
        return kuduClient;
    }

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);
        //#create-converter
        List<ColumnSchema> columns = new ArrayList(2);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).build());
        schema = new Schema(columns);
        //#create-converter

        //#create-settings
        List<String> rangeKeys = new ArrayList<>();
        rangeKeys.add("key");
        tableName = "test";
        createTableOptions = new CreateTableOptions().setNumReplicas(1).setRangePartitionColumns(rangeKeys);
        //#create-settings
    }

    @AfterClass
    public static void teardown() {
        TestKit.shutdownActorSystem(system);
    }

        //#create-converter
        Function<Person, PartialRow> kuduConverter = person -> {
            PartialRow partialRow = schema.newPartialRow();
            partialRow.addInt(0, person.id);
            partialRow.addString(1, person.name);
            return partialRow;
        };
        //#create-converter

    @Test
    public void sink() throws ExecutionException, InterruptedException, TimeoutException {

        //#create-settings
        KuduTableSettings<Person> tableSettings = KuduTableSettings.create(setupKuduClient(), tableName, schema, createTableOptions, kuduConverter);
        //#create-settings

        //#sink
        final Sink<Person, Future<Done>> sink = KuduTableStage.sink(tableSettings);
        Future<Done> o = Source.from(Arrays.asList(100, 101, 102, 103, 104)).map((i) -> new Person(i, String.format("name %d", i))).runWith(sink, materializer);
        //#sink

        Await.ready(o, Duration.Inf());
    }

    @Test
    public void flow() throws ExecutionException, InterruptedException {

        KuduTableSettings<Person> tableSettings = KuduTableSettings.create(setupKuduClient(), tableName, schema, createTableOptions, kuduConverter);

        //#flow
        Flow<Person, Person, NotUsed> flow = KuduTableStage.flow(tableSettings);
        Pair<NotUsed, CompletionStage<List<Person>>> run = Source.from(Arrays.asList(200, 201, 202, 203, 204)).map((i)
                -> new Person(i, String.format("name_%d", i))).via(flow).toMat(Sink.seq(), Keep.both()).run(materializer);
        //#flow

        run.second().toCompletableFuture().get();
    }

}

class Person {
    int id;
    String name;

    public Person(int i, String name) {
        this.id = i;
        this.name = name;
    }
}
