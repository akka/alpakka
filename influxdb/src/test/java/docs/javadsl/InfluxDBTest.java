/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.influxdb.InfluxDBSettings;
import akka.stream.alpakka.influxdb.InfluxDBWriteMessage;
import akka.stream.alpakka.influxdb.javadsl.InfluxDBSink;
import akka.stream.alpakka.influxdb.javadsl.InfluxDBSource;
import akka.stream.javadsl.Sink;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.javadsl.TestKit;
import static docs.javadsl.TestUtils.cleanDatabase;
import static docs.javadsl.TestUtils.dropDatabase;
import static docs.javadsl.TestUtils.populateDatabase;
import static docs.javadsl.TestUtils.resultToPoint;
import static docs.javadsl.TestUtils.setupConnection;

public class InfluxDBTest {

  private static ActorSystem system;
  private static Materializer materializer;
  private static InfluxDB influxDB;

  private static final String DATABASE_NAME = "InfluxDBTest";

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
    populateDatabase(influxDB, InfluxDBCpu.class);
  }

  @After
  public void cleanUp() {
    cleanDatabase(influxDB, DATABASE_NAME);
    StreamTestKit.assertAllStagesStopped(materializer);
  }

  @Test
  public void testConsumeAndPublishMeasurementsUsingTyped() throws Exception {
    Query query = new Query("SELECT * FROM cpu", DATABASE_NAME);
    CompletionStage<Done> completionStage =
        InfluxDBSource.typed(InfluxDBCpu.class, InfluxDBSettings.Default(), influxDB, query)
            .map(
                cpu -> {
                  InfluxDBCpu clonedCpu = cpu.cloneAt(cpu.getTime().plusSeconds(60000l));
                  return new InfluxDBWriteMessage<>(clonedCpu, NotUsed.notUsed());
                })
            .runWith(
                InfluxDBSink.typed(InfluxDBCpu.class, InfluxDBSettings.Default(), influxDB),
                materializer);

    Assert.assertNotNull(completionStage.toCompletableFuture().get());

    CompletionStage<List<Cpu>> sources =
        InfluxDBSource.typed(Cpu.class, InfluxDBSettings.Default(), influxDB, query)
            .runWith(Sink.seq(), materializer);

    Assert.assertEquals(4, sources.toCompletableFuture().get().size());
  }

  @Test
  public void testConsumeAndPublishMeasurements() throws Exception {
    Query query = new Query("SELECT * FROM cpu", DATABASE_NAME);

    CompletionStage<Done> completionStage =
        InfluxDBSource.create(influxDB, query)
            .map(queryResult -> points(queryResult))
            .mapConcat(i -> i)
            .runWith(InfluxDBSink.create(InfluxDBSettings.Default(), influxDB), materializer);

    Assert.assertNotNull(completionStage.toCompletableFuture().get());

    List<QueryResult> queryResult =
        InfluxDBSource.create(influxDB, query)
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get();
    final int resultSize =
        queryResult.get(0).getResults().get(0).getSeries().get(0).getValues().size();

    Assert.assertEquals(4, resultSize);
  }

  private List<InfluxDBWriteMessage<Point, NotUsed>> points(QueryResult queryResult) {
    List<InfluxDBWriteMessage<Point, NotUsed>> points = new ArrayList<>();

    for (QueryResult.Result result : queryResult.getResults()) {
      for (QueryResult.Series series : result.getSeries()) {
        for (List<Object> rows : series.getValues()) {
          InfluxDBWriteMessage<Point, NotUsed> influxDBWriteMessage =
              new InfluxDBWriteMessage<>(resultToPoint(series, rows), NotUsed.notUsed());
          points.add(influxDBWriteMessage);
        }
      }
    }

    return points;
  }
}
