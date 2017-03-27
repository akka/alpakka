/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.cassandra.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.stream.alpakka.cassandra.CassandraSourceStage;
import akka.stream.javadsl.Source;
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
import akka.testkit.*;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * All the tests must be run with a local Cassandra running on default port 9042.
 */
public class CassandraSourceTest {

  static ActorSystem system;
  static Materializer materializer;

  static Session session;

  public static Session setupSession() {
    //#init-session
    final Session session = Cluster.builder()
      .addContactPoint("127.0.0.1").withPort(9042)
      .build().connect();
    //#init-session
    return session;
  }

  public static Pair<ActorSystem, Materializer> setupMaterializer() {
    //#init-mat
    final ActorSystem system = ActorSystem.create();
    final Materializer materializer = ActorMaterializer.create(system);
    //#init-mat
    return Pair.create(system, materializer);
  }

  @BeforeClass
  public static void setup() {
    final Pair<ActorSystem, Materializer> sysmat = setupMaterializer();
    system = sysmat.first();
    materializer = sysmat.second();

    session = setupSession();

    session.execute(
      "CREATE KEYSPACE IF NOT EXISTS akka_stream_java_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}"
    );
    session.execute(
      "CREATE TABLE IF NOT EXISTS akka_stream_java_test.test (id int PRIMARY KEY);"
    );
  }

  @AfterClass
  public static void teardown() {
    session.execute("DROP TABLE IF EXISTS akka_stream_java_test.test;");
    session.execute("DROP KEYSPACE IF EXISTS akka_stream_java_test;");

    JavaTestKit.shutdownActorSystem(system);
  }

  @After
  public void cleanUp() {
    session.execute("truncate akka_stream_java_test.test");
  }

  @Test
  public void streamStatementResult() throws Exception {
    for (Integer i = 1; i < 103; i++) {
      session.execute("INSERT INTO akka_stream_java_test.test(id) VALUES (" + i + ")");
    }

    //#statement
    final Statement stmt = new SimpleStatement("SELECT * FROM akka_stream_java_test.test").setFetchSize(20);
    //#statement

    //#run-source
    final CompletionStage<List<Row>> rows = CassandraSource.create(stmt, session)
      .runWith(Sink.seq(), materializer);
    //#run-source

    assertEquals(
      IntStream.range(1, 103).boxed().collect(Collectors.toSet()),
      rows.toCompletableFuture().get(3, TimeUnit.SECONDS).stream().map(r -> r.getInt("id")).collect(Collectors.toSet()));
  }

  @Test
  public void sinkInputValues() throws Exception {

    //#prepared-statement
    final PreparedStatement preparedStatement = session.prepare("insert into akka_stream_java_test.test (id) values (?)");
    //#prepared-statement

    //#statement-binder
    BiFunction<Integer, PreparedStatement,BoundStatement> statementBinder = (myInteger, statement) -> {
      return statement.bind(myInteger);
    };
    //#statement-binder

    Source<Integer, NotUsed> source = Source.from(IntStream.range(1, 10).boxed().collect(Collectors.toList()));


    //#run-sink
    final Sink<Integer, CompletionStage<Done>> sink = CassandraSink.create(2, preparedStatement, statementBinder, session, system.dispatcher());

    CompletionStage<Done> result = source.runWith(sink, materializer);
    //#run-sink

    result.toCompletableFuture().get();

    Set<Integer> found = session.execute("select * from akka_stream_java_test.test").all().stream().map(r -> r.getInt("id")).collect(Collectors.toSet());

    assertEquals(found, IntStream.range(1, 10).boxed().collect(Collectors.toSet()));
  }
}
