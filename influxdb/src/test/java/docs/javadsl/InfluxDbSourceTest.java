/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import java.util.List;
import java.util.concurrent.CompletionStage;

import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.*;

import akka.actor.ActorSystem;
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
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static ActorSystem system;
  private static InfluxDB influxDB;

  private static final String DATABASE_NAME = "InfluxDbSourceTest";

  @BeforeClass
  public static void setupDatabase() {
    system = ActorSystem.create();

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
    StreamTestKit.assertAllStagesStopped(Materializer.matFromSystem(system));
  }

  @Test
  public void streamQueryResult() throws Exception {
    Query query = new Query("SELECT * FROM cpu", DATABASE_NAME);

    CompletionStage<List<InfluxDbSourceCpu>> rows =
        InfluxDbSource.typed(
                InfluxDbSourceCpu.class, InfluxDbReadSettings.Default(), influxDB, query)
            .runWith(Sink.seq(), system);

    List<InfluxDbSourceCpu> cpus = rows.toCompletableFuture().get();

    Assert.assertEquals(2, cpus.size());
  }

  @Test
  public void streamRawQueryResult() throws Exception {
    Query query = new Query("SELECT * FROM cpu", DATABASE_NAME);

    CompletionStage<List<QueryResult>> completionStage =
        InfluxDbSource.create(influxDB, query).runWith(Sink.seq(), system);

    List<QueryResult> queryResults = completionStage.toCompletableFuture().get();
    QueryResult queryResult = queryResults.get(0);

    Assert.assertFalse(queryResult.hasError());

    final int resultSize = queryResult.getResults().get(0).getSeries().get(0).getValues().size();

    Assert.assertEquals(2, resultSize);
  }
}
