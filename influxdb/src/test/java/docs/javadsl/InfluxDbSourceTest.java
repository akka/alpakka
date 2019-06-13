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
import akka.stream.alpakka.influxdb.InfluxDbReadSettings;
import akka.stream.alpakka.influxdb.javadsl.InfluxDbSource;
import akka.stream.javadsl.Sink;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.javadsl.TestKit;
import static docs.javadsl.TestUtils.cleanDatabase;
import static docs.javadsl.TestUtils.dropDatabase;
import static docs.javadsl.TestUtils.populateDatabase;
import static docs.javadsl.TestUtils.setupConnection;

public class InfluxDbSourceTest {

  private static ActorSystem system;
  private static Materializer materializer;
  private static InfluxDB influxDB;

  private static final String DATABASE_NAME = "InfluxDbSourceTest";

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

    influxDB = setupConnection(DATABASE_NAME);
  }

  @AfterClass
  public static void teardown() {
    dropDatabase(influxDB, DATABASE_NAME);
    TestKit.shutdownActorSystem(system);
  }

  @Before
  public void setUp() throws Exception {
    populateDatabase(influxDB, InfluxDbSourceCpu.class);
  }

  @After
  public void cleanUp() {
    cleanDatabase(influxDB, DATABASE_NAME);
    StreamTestKit.assertAllStagesStopped(materializer);
  }

  @Test
  public void streamQueryResult() throws Exception {
    Query query = new Query("SELECT * FROM cpu", DATABASE_NAME);

    CompletionStage<List<InfluxDbSourceCpu>> rows =
        InfluxDbSource.typed(
                InfluxDbSourceCpu.class, InfluxDbReadSettings.Default(), influxDB, query)
            .runWith(Sink.seq(), materializer);

    List<InfluxDbSourceCpu> cpus = rows.toCompletableFuture().get();

    Assert.assertEquals(2, cpus.size());
  }

  @Test
  public void streamRawQueryResult() throws Exception {
    Query query = new Query("SELECT * FROM cpu", DATABASE_NAME);

    CompletionStage<List<QueryResult>> completionStage =
        InfluxDbSource.create(influxDB, query).runWith(Sink.seq(), materializer);

    List<QueryResult> queryResults = completionStage.toCompletableFuture().get();
    QueryResult queryResult = queryResults.get(0);

    Assert.assertFalse(queryResult.hasError());

    final int resultSize = queryResult.getResults().get(0).getSeries().get(0).getValues().size();

    Assert.assertEquals(2, resultSize);
  }
}
