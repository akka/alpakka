/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package docs.javadsl;

import static org.junit.Assert.assertEquals;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.alpakka.hbase.HTableSettings;
import akka.stream.alpakka.hbase.javadsl.HTableStage;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class HBaseStageTest {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create();
  }

  @AfterClass
  public static void teardown() {
    TestKit.shutdownActorSystem(system);
  }

  // #create-converter-put
  Function<Person, List<Mutation>> hBaseConverter =
      person -> {
        try {
          Put put = new Put(String.format("id_%d", person.id).getBytes("UTF-8"));
          put.addColumn(
              "info".getBytes("UTF-8"), "name".getBytes("UTF-8"), person.name.getBytes("UTF-8"));

          return Collections.singletonList(put);
        } catch (UnsupportedEncodingException e) {
          e.printStackTrace();
          return Collections.emptyList();
        }
      };
  // #create-converter-put

  // #create-converter-append
  Function<Person, List<Mutation>> appendHBaseConverter =
      person -> {
        try {
          Append append = new Append(String.format("id_%d", person.id).getBytes("UTF-8"));
          append.addColumn(
              "info".getBytes("UTF-8"), "aliases".getBytes("UTF-8"), person.name.getBytes("UTF-8"));

          return Collections.singletonList(append);
        } catch (UnsupportedEncodingException e) {
          e.printStackTrace();
          return Collections.emptyList();
        }
      };
  // #create-converter-append

  // #create-converter-delete
  Function<Person, List<Mutation>> deleteHBaseConverter =
      person -> {
        try {
          Delete delete = new Delete(String.format("id_%d", person.id).getBytes("UTF-8"));

          return Collections.singletonList(delete);
        } catch (UnsupportedEncodingException e) {
          e.printStackTrace();
          return Collections.emptyList();
        }
      };
  // #create-converter-delete

  // #create-converter-increment
  Function<Person, List<Mutation>> incrementHBaseConverter =
      person -> {
        try {
          Increment increment = new Increment(String.format("id_%d", person.id).getBytes("UTF-8"));
          increment.addColumn("info".getBytes("UTF-8"), "numberOfChanges".getBytes("UTF-8"), 1);

          return Collections.singletonList(increment);
        } catch (UnsupportedEncodingException e) {
          e.printStackTrace();
          return Collections.emptyList();
        }
      };
  // #create-converter-increment

  // #create-converter-complex
  Function<Person, List<Mutation>> complexHBaseConverter =
      person -> {
        try {
          byte[] id = String.format("id_%d", person.id).getBytes("UTF-8");
          byte[] infoFamily = "info".getBytes("UTF-8");

          if (person.id != 0 && person.name.isEmpty()) {
            Delete delete = new Delete(id);
            return Collections.singletonList(delete);
          } else if (person.id != 0) {
            Put put = new Put(id);
            put.addColumn(infoFamily, "name".getBytes("UTF-8"), person.name.getBytes("UTF-8"));

            Increment increment = new Increment(id);
            increment.addColumn(infoFamily, "numberOfChanges".getBytes("UTF-8"), 1);

            return Arrays.asList(put, increment);
          } else {
            return Collections.emptyList();
          }
        } catch (UnsupportedEncodingException e) {
          e.printStackTrace();
          return Collections.emptyList();
        }
      };
  // #create-converter-complex

  @Test
  public void writeToSink() throws InterruptedException, TimeoutException, ExecutionException {

    // #create-settings
    HTableSettings<Person> tableSettings =
        HTableSettings.create(
            HBaseConfiguration.create(),
            TableName.valueOf("person1"),
            Collections.singletonList("info"),
            hBaseConverter);
    // #create-settings

    // #sink
    final Sink<Person, CompletionStage<Done>> sink = HTableStage.sink(tableSettings);
    CompletionStage<Done> o =
        Source.from(Arrays.asList(100, 101, 102, 103, 104))
            .map((i) -> new Person(i, String.format("name %d", i)))
            .runWith(sink, system);
    // #sink

    assertEquals(Done.getInstance(), o.toCompletableFuture().get(5, TimeUnit.SECONDS));
  }

  @Test
  public void writeThroughFlow() throws ExecutionException, InterruptedException {

    HTableSettings<Person> tableSettings =
        HTableSettings.create(
            HBaseConfiguration.create(),
            TableName.valueOf("person2"),
            Collections.singletonList("info"),
            hBaseConverter);

    // #flow
    Flow<Person, Person, NotUsed> flow = HTableStage.flow(tableSettings);
    Pair<NotUsed, CompletionStage<List<Person>>> run =
        Source.from(Arrays.asList(200, 201, 202, 203, 204))
            .map((i) -> new Person(i, String.format("name_%d", i)))
            .via(flow)
            .toMat(Sink.seq(), Keep.both())
            .run(system);
    // #flow

    assertEquals(5, run.second().toCompletableFuture().get().size());
  }

  @Test
  public void readFromSource()
      throws InterruptedException, TimeoutException, ExecutionException,
          UnsupportedEncodingException {

    HTableSettings<Person> tableSettings =
        HTableSettings.create(
            HBaseConfiguration.create(),
            TableName.valueOf("person1"),
            Collections.singletonList("info"),
            hBaseConverter);

    final Sink<Person, CompletionStage<Done>> sink = HTableStage.sink(tableSettings);
    CompletionStage<Done> o =
        Source.from(Arrays.asList(new Person(300, "name 300"))).runWith(sink, system);
    assertEquals(Done.getInstance(), o.toCompletableFuture().get(5, TimeUnit.SECONDS));

    // #source
    Scan scan = new Scan(new Get("id_300".getBytes("UTF-8")));

    CompletionStage<List<Result>> f =
        HTableStage.source(scan, tableSettings).runWith(Sink.seq(), system);
    // #source

    assertEquals(1, f.toCompletableFuture().get().size());
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
