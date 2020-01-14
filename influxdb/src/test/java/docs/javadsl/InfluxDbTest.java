/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
import akka.stream.alpakka.influxdb.InfluxDbWriteResult;
import akka.stream.alpakka.influxdb.javadsl.InfluxDbFlow;
import akka.stream.alpakka.influxdb.javadsl.InfluxDbSink;
import akka.stream.alpakka.influxdb.javadsl.InfluxDbSource;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.javadsl.TestKit;
import static docs.javadsl.TestUtils.cleanDatabase;
import static docs.javadsl.TestUtils.dropDatabase;
import static docs.javadsl.TestUtils.populateDatabase;
import static docs.javadsl.TestUtils.resultToPoint;
import static docs.javadsl.TestUtils.setupConnection;
import static org.junit.Assert.assertEquals;

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

  public static class MessageFromKafka {

    private InfluxDbCpu influxDbCpu;

    private KafkaOffset kafkaOffset;

    public MessageFromKafka(InfluxDbCpu influxDbCpu, KafkaOffset kafkaOffset) {
      this.influxDbCpu = influxDbCpu;
      this.kafkaOffset = kafkaOffset;
    }
  }

  public static class KafkaOffset {

    private int offset;

    public KafkaOffset(int offset) {
      this.offset = offset;
    }

    public void setOffset(int offset) {
      this.offset = offset;
    }

    public int getOffset() {
      return offset;
    }
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
    // #run-typed
    CompletionStage<Done> completionStage =
        InfluxDbSource.typed(InfluxDbCpu.class, InfluxDbReadSettings.Default(), influxDB, query)
            .map(
                cpu -> {
                  InfluxDbCpu clonedCpu = cpu.cloneAt(cpu.getTime().plusSeconds(60000l));
                  return InfluxDbWriteMessage.create(clonedCpu, NotUsed.notUsed());
                })
            .groupedWithin(10, Duration.of(50l, ChronoUnit.MILLIS))
            .runWith(InfluxDbSink.typed(InfluxDbCpu.class, influxDB), materializer);
    // #run-typed

    Assert.assertNotNull(completionStage.toCompletableFuture().get());

    CompletionStage<List<Cpu>> sources =
        InfluxDbSource.typed(Cpu.class, InfluxDbReadSettings.Default(), influxDB, query)
            .runWith(Sink.seq(), materializer);

    assertEquals(4, sources.toCompletableFuture().get().size());
  }

  @Test
  public void testConsumeAndPublishMeasurements() throws Exception {
    // #run-query-result
    Query query = new Query("SELECT * FROM cpu", DATABASE_NAME);

    CompletionStage<Done> completionStage =
        InfluxDbSource.create(influxDB, query)
            .map(queryResult -> points(queryResult))
            .mapConcat(i -> i)
            .groupedWithin(10, Duration.of(50l, ChronoUnit.MILLIS))
            .runWith(InfluxDbSink.create(influxDB), materializer);
    // #run-query-result

    Assert.assertNotNull(completionStage.toCompletableFuture().get());

    List<QueryResult> queryResult =
        InfluxDbSource.create(influxDB, query)
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get();
    final int resultSize =
        queryResult.get(0).getResults().get(0).getSeries().get(0).getValues().size();

    assertEquals(4, resultSize);
  }

  @Test
  public void testPointFlow() throws Exception {
    Point point =
        Point.measurement("disk")
            .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
            .addField("used", 80L)
            .addField("free", 1L)
            .build();

    InfluxDbWriteMessage<Point, NotUsed> influxDbWriteMessage = InfluxDbWriteMessage.create(point);

    // #run-flow
    CompletableFuture<List<List<InfluxDbWriteResult<Point, NotUsed>>>> completableFuture =
        Source.single(Collections.singletonList(influxDbWriteMessage))
            .via(InfluxDbFlow.create(influxDB))
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture();
    // #run-flow

    InfluxDbWriteResult<Point, NotUsed> influxDbWriteResult = completableFuture.get().get(0).get(0);
    Assert.assertTrue(influxDbWriteResult.error().isEmpty());
  }

  @Test
  public void typedStreamWithPassThrough() throws Exception {
    // #kafka-example
    // We're going to pretend we got metrics from kafka.
    // After we've written them to InfluxDb, we want
    // to commit the offset to Kafka

    /** Just clean the previous data */
    influxDB.query(new Query("DELETE FROM cpu"));

    List<Integer> committedOffsets = new ArrayList<>();
    List<MessageFromKafka> messageFromKafka =
        Arrays.asList(
            new MessageFromKafka(
                new InfluxDbCpu(
                    Instant.now().minusSeconds(1000), "local_1", "eu-west-2", 1.4d, true, 123L),
                new KafkaOffset(0)),
            new MessageFromKafka(
                new InfluxDbCpu(
                    Instant.now().minusSeconds(2000), "local_2", "eu-west-1", 2.5d, false, 125L),
                new KafkaOffset(1)),
            new MessageFromKafka(
                new InfluxDbCpu(
                    Instant.now().minusSeconds(3000), "local_3", "eu-west-4", 3.1d, false, 251L),
                new KafkaOffset(2)));

    Consumer<KafkaOffset> commitToKafka =
        kafkaOffset -> committedOffsets.add(kafkaOffset.getOffset());

    Source.from(messageFromKafka)
        .map(
            kafkaMessage -> {
              return InfluxDbWriteMessage.create(
                  kafkaMessage.influxDbCpu, kafkaMessage.kafkaOffset);
            })
        .groupedWithin(10, Duration.ofMillis(10))
        .via(InfluxDbFlow.typedWithPassThrough(InfluxDbCpu.class, influxDB))
        .map(
            messages -> {
              messages.stream()
                  .forEach(
                      message -> {
                        KafkaOffset kafkaOffset = message.writeMessage().passThrough();
                        commitToKafka.accept(kafkaOffset);
                      });
              return NotUsed.getInstance();
            })
        .runWith(Sink.seq(), materializer)
        .toCompletableFuture()
        .get(10, TimeUnit.SECONDS);
    // #kafka-example

    assertEquals(Arrays.asList(0, 1, 2), committedOffsets);

    List<String> result2 =
        InfluxDbSource.typed(
                InfluxDbCpu.class,
                InfluxDbReadSettings.Default(),
                influxDB,
                new Query("SELECT*FROM cpu"))
            .map(m -> m.getHostname())
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get(10, TimeUnit.SECONDS);

    assertEquals(
        messageFromKafka.stream()
            .map(m -> m.influxDbCpu.getHostname())
            .sorted()
            .collect(Collectors.toList()),
        result2.stream().sorted().collect(Collectors.toList()));
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
