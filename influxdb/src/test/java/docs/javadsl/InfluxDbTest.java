/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

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
import akka.stream.alpakka.influxdb.InfluxDbReadSettings;
import akka.stream.alpakka.influxdb.InfluxDbWriteMessage;
import akka.stream.alpakka.influxdb.javadsl.InfluxDbSink;
import akka.stream.alpakka.influxdb.javadsl.InfluxDbSource;
import akka.stream.javadsl.Sink;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.javadsl.TestKit;
import static docs.javadsl.TestUtils.cleanDatabase;
import static docs.javadsl.TestUtils.dropDatabase;
import static docs.javadsl.TestUtils.populateDatabase;
import static docs.javadsl.TestUtils.resultToPoint;
import static docs.javadsl.TestUtils.setupConnection;

public class InfluxDbTest {

  private static ActorSystem system;
  private static Materializer materializer;
  private static InfluxDB influxDB;

  private static final String DATABASE_NAME = "InfluxDbTest";

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
    populateDatabase(influxDB, InfluxDbCpu.class);
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
        InfluxDbSource.typed(InfluxDbCpu.class, InfluxDbReadSettings.Default(), influxDB, query)
            .map(
                cpu -> {
                  InfluxDbCpu clonedCpu = cpu.cloneAt(cpu.getTime().plusSeconds(60000l));
                  return InfluxDbWriteMessage.create(clonedCpu, NotUsed.notUsed());
                })
            .groupedWithin(10, Duration.of(50l, ChronoUnit.MILLIS))
            .runWith(InfluxDbSink.typed(InfluxDbCpu.class, influxDB), materializer);

    Assert.assertNotNull(completionStage.toCompletableFuture().get());

    CompletionStage<List<Cpu>> sources =
        InfluxDbSource.typed(Cpu.class, InfluxDbReadSettings.Default(), influxDB, query)
            .runWith(Sink.seq(), materializer);

    Assert.assertEquals(4, sources.toCompletableFuture().get().size());
  }

  @Test
  public void testConsumeAndPublishMeasurements() throws Exception {
    Query query = new Query("SELECT * FROM cpu", DATABASE_NAME);

    CompletionStage<Done> completionStage =
        InfluxDbSource.create(influxDB, query)
            .map(queryResult -> points(queryResult))
            .mapConcat(i -> i)
            .groupedWithin(10, Duration.of(50l, ChronoUnit.MILLIS))
            .runWith(InfluxDbSink.create(influxDB), materializer);

    Assert.assertNotNull(completionStage.toCompletableFuture().get());

    List<QueryResult> queryResult =
        InfluxDbSource.create(influxDB, query)
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get();
    final int resultSize =
        queryResult.get(0).getResults().get(0).getSeries().get(0).getValues().size();

    Assert.assertEquals(4, resultSize);
  }

  private List<InfluxDbWriteMessage<Point, NotUsed>> points(QueryResult queryResult) {
    List<InfluxDbWriteMessage<Point, NotUsed>> points = new ArrayList<>();

    for (QueryResult.Result result : queryResult.getResults()) {
      for (QueryResult.Series series : result.getSeries()) {
        for (List<Object> rows : series.getValues()) {
          InfluxDbWriteMessage<Point, NotUsed> influxDBWriteMessage =
              InfluxDbWriteMessage.create(resultToPoint(series, rows), NotUsed.notUsed());
          points.add(influxDBWriteMessage);
        }
      }
    }

    return points;
  }
}
