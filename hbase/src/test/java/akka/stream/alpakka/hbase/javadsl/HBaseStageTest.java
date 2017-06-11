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
import akka.stream.alpakka.hbase.Utils.DNSUtils;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.JavaTestKit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * Created by olivier.nouguier@gmail.com on 27/11/2016.
 */
public class HBaseStageTest {

    static ActorSystem system;
    static Materializer materializer;

    private Configuration config;

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);
        DNSUtils.setupDNS("hbase");
    }

    @AfterClass
    public static void teardown() {
        JavaTestKit.shutdownActorSystem(system);
    }

    @Before
    public void setUp() throws Exception {
        config = HBaseConfiguration.create();
        config.clear();
        config.set("hbase.zookeeper.quorum", "localhost");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.master.port", "60000");
    }

    //#create-converter
    private Function<Person, Put> hBaseConverter = person -> {
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

    //#create-multi-put-converter
    private Function<Person, List<Put>> hBaseMultiPutConverter = person -> {
        List<Put> puts = new ArrayList<>();
        try {
            Put put = new Put(String.format("id_%d", person.id).getBytes("UTF-8"));
            put.addColumn("info".getBytes("UTF-8"), "name".getBytes("UTF-8"), person.name.getBytes("UTF-8"));
            puts.add(put);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return puts;
    };
    //#create-multi-put-converter


    @Test
    public void sink() throws ExecutionException, InterruptedException, TimeoutException, IOException {

        //#create-settings
        TableName table = TableName.valueOf("person1");
        HTableSettings<Person> tableSettings = HTableSettings.create(config, table, Collections.singletonList("info"), hBaseConverter);
        //#create-settings

        //#sink
        final Sink<Person, scala.concurrent.Future<Done>> sink = HTableStage.sink(tableSettings);
        Future<Done> o = Source.from(Arrays.asList(100, 101, 102, 103, 104)).map((i) -> new Person(i, String.format("name %d", i))).runWith(sink, materializer);
        //#sink

        Await.ready(o, Duration.Inf());
    }

    @Test
    public void flow() throws ExecutionException, InterruptedException, IOException {

        TableName table = TableName.valueOf("person2");
        HTableSettings<Person> tableSettings = HTableSettings.create(config, table, Collections.singletonList("info"), hBaseConverter);

        //#flow
        Flow<Person, Person, NotUsed> flow = HTableStage.flow(tableSettings);
        Pair<NotUsed, CompletionStage<List<Person>>> run = Source.from(Arrays.asList(200, 201, 202, 203, 204)).map((i)
                -> new Person(i, String.format("name_%d", i))).via(flow).toMat(Sink.seq(), Keep.both()).run(materializer);
        //#flow

        run.second().toCompletableFuture().get();
    }

    @Test
    public void sinkWithMultiPuts() throws Exception {
        //#create-settings
        TableName table = TableName.valueOf("person1");
        HTableSettings<Person> tableSettings = HTableSettings.createMulti(config, table, Collections.singletonList("info"), hBaseMultiPutConverter);
        //#create-settings

        //#sink
        final Sink<Person, scala.concurrent.Future<Done>> sink = HTableStage.sink(tableSettings);
        Future<Done> o = Source.from(Arrays.asList(100, 101, 102, 103, 104)).map((i) -> new Person(i, String.format("name %d", i))).runWith(sink, materializer);
        //#sink

        Await.ready(o, Duration.Inf());
    }

    @Test
    public void sinkWithMultiPutsSettingsWithTable() throws Exception {
        //#create-settings
        TableName table = TableName.valueOf("person1");
        HTableSettings<Person> tableSettings = HTableStage.tableMulti(config, table, Collections.singletonList("info"), hBaseMultiPutConverter);
        //#create-settings

        //#sink
        final Sink<Person, scala.concurrent.Future<Done>> sink = HTableStage.sink(tableSettings);
        Future<Done> o = Source.from(Arrays.asList(100, 101, 102, 103, 104)).map((i) -> new Person(i, String.format("name %d", i))).runWith(sink, materializer);
        //#sink

        Await.ready(o, Duration.Inf());
    }

    @Test
    public void flowWithMultiPuts() throws ExecutionException, InterruptedException, IOException {

        TableName table = TableName.valueOf("person2");
        HTableSettings<Person> tableSettings = HTableSettings.createMulti(config, table, Collections.singletonList("info"), hBaseMultiPutConverter);

        //#flow
        Flow<Person, Person, NotUsed> flow = HTableStage.flow(tableSettings);
        Pair<NotUsed, CompletionStage<List<Person>>> run = Source.from(Arrays.asList(200, 201, 202, 203, 204)).map((i)
                -> new Person(i, String.format("name_%d", i))).via(flow).toMat(Sink.seq(), Keep.both()).run(materializer);
        //#flow

        run.second().toCompletableFuture().get();
    }

    @Test
    public void flowWithMultiPutsSettingsWithTable() throws ExecutionException, InterruptedException, IOException {

        TableName table = TableName.valueOf("person2");
        HTableSettings<Person> tableSettings = HTableStage.tableMulti(config, table, Collections.singletonList("info"), hBaseMultiPutConverter);

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
