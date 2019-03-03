/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.stream.alpakka.cassandra.CassandraBatchSettings;
import akka.stream.alpakka.cassandra.javadsl.CassandraFlow;
import akka.stream.alpakka.cassandra.javadsl.CassandraSink;
import akka.stream.alpakka.cassandra.javadsl.CassandraSource;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.javadsl.TestKit;
import com.datastax.driver.core.*;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** All the tests must be run with a local Cassandra running on default port 9042. */
public class CassandraSourceTest {

  // #element-to-insert
  private class ToInsert {
    Integer id;
    Integer cc;

    public ToInsert(Integer id, Integer cc) {
      this.id = id;
      this.cc = cc;
    }
  }
  // #element-to-insert

  private static ActorSystem system;
  private static Materializer materializer;
  private static Session session;

  private static Session setupSession() {
    // #init-session
    final Session session =
        Cluster.builder().addContactPoint("127.0.0.1").withPort(9042).build().connect();
    // #init-session
    return session;
  }

  private static Pair<ActorSystem, Materializer> setupMaterializer() {
    // #init-mat
    final ActorSystem system = ActorSystem.create();
    final Materializer materializer = ActorMaterializer.create(system);
    // #init-mat
    return Pair.create(system, materializer);
  }

  @BeforeClass
  public static void setup() {
    final Pair<ActorSystem, Materializer> sysmat = setupMaterializer();
    system = sysmat.first();
    materializer = sysmat.second();

    session = setupSession();

    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS akka_stream_java_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
    session.execute("CREATE TABLE IF NOT EXISTS akka_stream_java_test.test (id int PRIMARY KEY);");
    session.execute(
        "CREATE TABLE IF NOT EXISTS akka_stream_java_test.test_batch (id int, cc int, PRIMARY KEY (id, cc));");
  }

  @AfterClass
  public static void teardown() {
    session.execute("DROP TABLE IF EXISTS akka_stream_java_test.test;");
    session.execute("DROP KEYSPACE IF EXISTS akka_stream_java_test;");

    TestKit.shutdownActorSystem(system);
  }

  @After
  public void cleanUp() {
    session.execute("truncate akka_stream_java_test.test");
    StreamTestKit.assertAllStagesStopped(materializer);
  }

  @Test
  public void streamStatementResult() throws Exception {
    for (Integer i = 1; i < 103; i++) {
      session.execute("INSERT INTO akka_stream_java_test.test(id) VALUES (" + i + ")");
    }

    // #statement
    final Statement stmt =
        new SimpleStatement("SELECT * FROM akka_stream_java_test.test").setFetchSize(20);
    // #statement

    // #run-source
    final CompletionStage<List<Row>> rows =
        CassandraSource.create(stmt, session).runWith(Sink.seq(), materializer);
    // #run-source

    assertEquals(
        IntStream.range(1, 103).boxed().collect(Collectors.toSet()),
        rows.toCompletableFuture()
            .get(3, TimeUnit.SECONDS)
            .stream()
            .map(r -> r.getInt("id"))
            .collect(Collectors.toSet()));
  }

  @Test
  public void flowInputValues() throws Exception {

    // #prepared-statement-flow
    final PreparedStatement preparedStatement =
        session.prepare("insert into akka_stream_java_test.test (id) values (?)");
    // #prepared-statement-flow

    // #statement-binder-flow
    BiFunction<Integer, PreparedStatement, BoundStatement> statementBinder =
        (myInteger, statement) -> statement.bind(myInteger);
    // #statement-binder-flow
    Source<Integer, NotUsed> source =
        Source.from(IntStream.range(1, 10).boxed().collect(Collectors.toList()));

    // #run-flow
    final Flow<Integer, Integer, NotUsed> flow =
        CassandraFlow.createWithPassThrough(2, preparedStatement, statementBinder, session);

    CompletionStage<List<Integer>> result = source.via(flow).runWith(Sink.seq(), materializer);
    // #run-flow

    List<Integer> resultToAssert = result.toCompletableFuture().get();
    Set<Integer> found =
        session
            .execute("select * from akka_stream_java_test.test")
            .all()
            .stream()
            .map(r -> r.getInt("id"))
            .collect(Collectors.toSet());

    assertEquals(resultToAssert, IntStream.range(1, 10).boxed().collect(Collectors.toList()));
    assertEquals(found, IntStream.range(1, 10).boxed().collect(Collectors.toSet()));
  }

  @Test
  public void flowBatchInputValues() throws Exception {

    // #prepared-statement-batching-flow
    final PreparedStatement preparedStatement =
        session.prepare("insert into akka_stream_java_test.test_batch(id, cc) values (?, ?)");
    // #prepared-statement-batching-flow

    // #statement-binder-batching-flow
    BiFunction<ToInsert, PreparedStatement, BoundStatement> statementBinder =
        (toInsert, statement) -> statement.bind(toInsert.id, toInsert.cc);
    // #statement-binder-batching-flow
    Source<ToInsert, NotUsed> source =
        Source.from(
            IntStream.range(1, 100)
                .boxed()
                .map(i -> new ToInsert(i % 2, i))
                .collect(Collectors.toList()));

    // #settings-batching-flow
    CassandraBatchSettings defaultSettings = CassandraBatchSettings.create();
    // #settings-batching-flow

    // #run-batching-flow
    final Flow<ToInsert, ToInsert, NotUsed> flow =
        CassandraFlow.createUnloggedBatchWithPassThrough(
            2, preparedStatement, statementBinder, (ti) -> ti.id, defaultSettings, session);

    CompletionStage<List<ToInsert>> result = source.via(flow).runWith(Sink.seq(), materializer);
    // #run-batching-flow

    Set<Integer> resultToAssert =
        result.toCompletableFuture().get().stream().map(ti -> ti.cc).collect(Collectors.toSet());
    Set<Integer> found =
        session
            .execute("select * from akka_stream_java_test.test_batch")
            .all()
            .stream()
            .map(r -> r.getInt("cc"))
            .collect(Collectors.toSet());

    assertEquals(resultToAssert, IntStream.range(1, 100).boxed().collect(Collectors.toSet()));
    assertEquals(found, IntStream.range(1, 100).boxed().collect(Collectors.toSet()));
  }

  @Test
  public void sinkInputValues() throws Exception {

    // #prepared-statement
    final PreparedStatement preparedStatement =
        session.prepare("insert into akka_stream_java_test.test (id) values (?)");
    // #prepared-statement

    // #statement-binder
    BiFunction<Integer, PreparedStatement, BoundStatement> statementBinder =
        (myInteger, statement) -> statement.bind(myInteger);
    // #statement-binder

    Source<Integer, NotUsed> source =
        Source.from(IntStream.range(1, 10).boxed().collect(Collectors.toList()));

    // #run-sink
    final Sink<Integer, CompletionStage<Done>> sink =
        CassandraSink.create(2, preparedStatement, statementBinder, session);

    CompletionStage<Done> result = source.runWith(sink, materializer);
    // #run-sink

    result.toCompletableFuture().get();

    Set<Integer> found =
        session
            .execute("select * from akka_stream_java_test.test")
            .all()
            .stream()
            .map(r -> r.getInt("id"))
            .collect(Collectors.toSet());
    assertEquals(found, IntStream.range(1, 10).boxed().collect(Collectors.toSet()));
  }
}
