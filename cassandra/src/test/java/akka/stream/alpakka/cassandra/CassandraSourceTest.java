/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.cassandra;

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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.Row;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
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
    for (Integer i = 1; i < 103; i++) {
      session.execute("INSERT INTO akka_stream_java_test.test(id) VALUES (" + i + ")");
    }
  }

  @AfterClass
  public static void teardown() {
    session.execute("DROP TABLE IF EXISTS akka_stream_java_test.test;");
    session.execute("DROP KEYSPACE IF EXISTS akka_stream_java_test;");

    JavaTestKit.shutdownActorSystem(system);
  }

  @Test
  public void streamStatementResult() throws Exception {
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

}
