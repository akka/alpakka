/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.javadsl;

import akka.Done;
import akka.stream.Attributes;
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryStorageSettings;
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryStorageSpecBase;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Sink;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.avro.generic.GenericRecord;
import org.junit.*;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

public class BigQueryStorageSpec extends BigQueryStorageSpecBase {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  @Test
  public void shouldReturnGenericRecordForAvroQuery()
      throws InterruptedException, ExecutionException, TimeoutException {
    CompletionStage<List<GenericRecord>> genericRecords =
        BigQueryStorage.read(Project(), Dataset(), Table())
            .withAttributes(mockBQReader())
            .flatMapMerge(100, i -> i)
            .runWith(Sink.seq(), system());

    assertTrue(
        "number of generic records should be more than 0",
        genericRecords.toCompletableFuture().get(5, TimeUnit.SECONDS).size() > 0);
  }

  @Test
  public void shouldFilterBasedOnRowRestriction()
      throws InterruptedException, ExecutionException, TimeoutException {
    ReadSession.TableReadOptions readOptions =
        ReadSession.TableReadOptions.newBuilder().setRowRestriction("true = false").build();
    CompletionStage<List<GenericRecord>> genericRecords =
        BigQueryStorage.read(Project(), Dataset(), Table(), readOptions)
            .withAttributes(mockBQReader())
            .flatMapMerge(100, i -> i)
            .runWith(Sink.seq(), system());

    assertEquals(
        "number of generic records should be filtered to 0",
        0,
        genericRecords.toCompletableFuture().get(5, TimeUnit.SECONDS).size());
  }

  @Test
  public void shouldFilterColumns()
      throws InterruptedException, ExecutionException, TimeoutException {
    ReadSession.TableReadOptions readOptions =
        ReadSession.TableReadOptions.newBuilder().addSelectedFields("col1").build();
    CompletionStage<List<GenericRecord>> genericRecords =
        BigQueryStorage.read(Project(), Dataset(), Table(), readOptions)
            .withAttributes(mockBQReader())
            .flatMapMerge(100, i -> i)
            .runWith(Sink.seq(), system());

    assertEquals(
        "fields of generic record should only include col1",
        Col1Record(),
        genericRecords.toCompletableFuture().get(5, TimeUnit.SECONDS).get(0));
  }

  @Test
  public void shouldRestrictNumberOfStreams()
      throws InterruptedException, ExecutionException, TimeoutException {
    Integer maxStreams = 5;
    CompletionStage<Integer> numStreams =
        BigQueryStorage.read(Project(), Dataset(), Table(), maxStreams)
            .withAttributes(mockBQReader())
            .runFold(0, (acc, stream) -> acc + 1, system());

    assertEquals(
        "the number of streams should be the same as specified in the request",
        maxStreams,
        numStreams.toCompletableFuture().get(5, TimeUnit.SECONDS));
  }

  @Test
  public void shouldFailIfBigQueryUnavailable()
      throws InterruptedException, ExecutionException, TimeoutException {
    CompletionStage<Done> result =
        BigQueryStorage.read(Project(), Dataset(), Table())
            .withAttributes(mockBQReader(1))
            .runWith(Sink.ignore(), system());

    try {
      result.toCompletableFuture().get(5, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      assertEquals(
          Status.Code.UNAVAILABLE, ((StatusRuntimeException) e.getCause()).getStatus().getCode());
    }
  }

  @Test
  public void shouldFailIfProjectIncorrect()
      throws InterruptedException, ExecutionException, TimeoutException {
    CompletionStage<Done> result =
        BigQueryStorage.read("NOT A PROJECT", Dataset(), Table())
            .withAttributes(mockBQReader())
            .runWith(Sink.ignore(), system());

    try {
      result.toCompletableFuture().get(5, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      assertEquals(
          Status.Code.INVALID_ARGUMENT,
          ((StatusRuntimeException) e.getCause()).getStatus().getCode());
    }
  }

  @Test
  public void shouldFailIfDatasetIncorrect()
      throws InterruptedException, ExecutionException, TimeoutException {
    CompletionStage<Done> result =
        BigQueryStorage.read(Project(), "NOT A DATASET", Table())
            .withAttributes(mockBQReader())
            .runWith(Sink.ignore(), system());

    try {
      result.toCompletableFuture().get(5, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      assertEquals(
          Status.Code.INVALID_ARGUMENT,
          ((StatusRuntimeException) e.getCause()).getStatus().getCode());
    }
  }

  @Test
  public void shouldFailIfTableIncorrect()
      throws InterruptedException, ExecutionException, TimeoutException {
    CompletionStage<Done> result =
        BigQueryStorage.read(Project(), Dataset(), "NOT A TABLE")
            .withAttributes(mockBQReader())
            .runWith(Sink.ignore(), system());

    try {
      result.toCompletableFuture().get(5, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      assertEquals(
          Status.Code.INVALID_ARGUMENT,
          ((StatusRuntimeException) e.getCause()).getStatus().getCode());
    }
  }

  public Attributes mockBQReader() {
    return mockBQReader(bqHost(), bqPort());
  }

  public Attributes mockBQReader(int port) {
    return mockBQReader(bqHost(), port);
  }

  public Attributes mockBQReader(String host, int port) {
    GrpcBigQueryStorageReader reader =
        GrpcBigQueryStorageReader.create(BigQueryStorageSettings.create(host, port), system());
    return BigQueryStorageAttributes.reader(reader);
  }

  @Before
  public void initialize() {
    startMock();
  }

  @After
  public void tearDown() {
    stopMock();
    system().terminate();
  }
}
