/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.influxdb.InfluxDBSettings;
import akka.stream.alpakka.influxdb.javadsl.InfluxDBSource;
import akka.stream.javadsl.Sink;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.javadsl.TestKit;
import static docs.javadsl.TestConstants.DATABASE_NAME;
import static docs.javadsl.TestUtils.cleanDatabase;
import static docs.javadsl.TestUtils.dropDatabase;
import static docs.javadsl.TestUtils.populateDatabase;
import static docs.javadsl.TestUtils.setupConnection;

public class InfluxDBSourceTest {

  private static ActorSystem system;
  private static Materializer materializer;
  private static InfluxDB influxDB;

  private static Pair<ActorSystem, Materializer> setupMaterializer() {
    // #init-mat
    final ActorSystem system = ActorSystem.create();
    final Materializer materializer = ActorMaterializer.create(system);
    // #init-mat
    return Pair.create(system, materializer);
  }

  @BeforeClass
  public static void setupDatabase() {
    final Pair<ActorSystem, Materializer> sysmat = setupMaterializer();
    system = sysmat.first();
    materializer = sysmat.second();

    influxDB = setupConnection();
  }

  @AfterClass
  public static void teardown() {
    dropDatabase(influxDB);
    TestKit.shutdownActorSystem(system);
  }

  @Before
  public void setUp() throws Exception {
    populateDatabase(influxDB);
  }

  @After
  public void cleanUp() {
    cleanDatabase(influxDB);
    StreamTestKit.assertAllStagesStopped(materializer);
  }

  @Test
  public void streamQueryResult() throws Exception {
    Query query = new Query("SELECT*FROM cpu", DATABASE_NAME);

    CompletionStage<List<Cpu>> rows =
        InfluxDBSource.typed(Cpu.class, InfluxDBSettings.Default(), influxDB, query)
            .runWith(Sink.seq(), materializer);

    List<Cpu> cpus = rows.toCompletableFuture().get();

    Assert.assertEquals(2, cpus.size());
  }

  @Test
  public void streamRawQueryResult() throws Exception {
    Query query = new Query("SELECT*FROM cpu", DATABASE_NAME);

    CompletionStage<List<QueryResult>> completionStage =
        InfluxDBSource.create(influxDB, query).runWith(Sink.seq(), materializer);

    List<QueryResult> queryResults = completionStage.toCompletableFuture().get();
    QueryResult queryResult = queryResults.get(0);

    Assert.assertFalse(queryResult.hasError());

    final int resultSize = queryResult.getResults().get(0).getSeries().get(0).getValues().size();

    Assert.assertEquals(2, resultSize);
  }
}
