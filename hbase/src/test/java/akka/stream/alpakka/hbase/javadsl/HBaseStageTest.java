/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.hbase.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.hbase.HTableSettings;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.JavaTestKit;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * Created by olivier.nouguier@gmail.com on 27/11/2016.
 */
@Ignore
public class HBaseStageTest {

    static ActorSystem system;
    static Materializer materializer;

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);
    }

    @AfterClass
    public static void teardown() {
        JavaTestKit.shutdownActorSystem(system);
    }

    //#create-converter
    Function<Person, Put> hBaseConverter = person -> {
        Put put = null;
        try {
            put = new Put(String.format("id_%d", person.id).getBytes("UTF-8"));
            put.addColumn("info".getBytes("UTF-8"), "name".getBytes("UTF-8"), person.name.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return put;
    };
    //#create-converter


    @Test
    public void sink() throws ExecutionException, InterruptedException, TimeoutException {

        //#create-settings
        HTableSettings<Person> tableSettings = HTableSettings.create(HBaseConfiguration.create(), TableName.valueOf("person1"), Arrays.asList("info"), hBaseConverter);
        //#create-settings

        //#sink
        final Sink<Person, scala.concurrent.Future<Done>> sink = HTableStage.sink(tableSettings);
        Future<Done> o = Source.from(Arrays.asList(100, 101, 102, 103, 104)).map((i) -> new Person(i, String.format("name %d", i))).runWith(sink, materializer);
        //#sink

        Await.ready(o, Duration.Inf());


    }

    @Test
    public void flow() throws ExecutionException, InterruptedException {

        HTableSettings<Person> tableSettings = HTableSettings.create(HBaseConfiguration.create(), TableName.valueOf("person2"), Arrays.asList("info"), hBaseConverter);

        //#flow
        Flow<Person, Person, NotUsed> flow = HTableStage.flow(tableSettings);
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
