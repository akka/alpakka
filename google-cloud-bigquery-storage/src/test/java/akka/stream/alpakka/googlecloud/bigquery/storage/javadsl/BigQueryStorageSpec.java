/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.stream.Attributes;
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryRecord;
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryStorageSettings;
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryStorageSpecBase;
import akka.stream.alpakka.googlecloud.bigquery.storage.impl.AvroDecoder;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Sink;

import com.google.cloud.bigquery.storage.v1.AvroSchema;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.avro.AvroRows;
import com.google.cloud.bigquery.storage.v1.storage.ReadRowsResponse;

import akka.stream.javadsl.Source;
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
import scala.Tuple2;
import scala.collection.immutable.Seq;

public class BigQueryStorageSpec extends BigQueryStorageSpecBase {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  @Test
  public void shouldReturnGenericRecordForAvroQuery()
      throws InterruptedException, ExecutionException, TimeoutException {
    AvroByteStringDecoder um = new AvroByteStringDecoder(Col1Schema());

    CompletionStage<List<List<BigQueryRecord>>> bigQueryRecords =
        BigQueryStorage.typed(Project(), Dataset(), Table(), DataFormat.AVRO, um)
            .withAttributes(mockBQReader())
            .runWith(Sink.seq(), system());

    assertTrue(
        "number of generic records should be more than 0",
        bigQueryRecords.toCompletableFuture().get(5, TimeUnit.SECONDS).get(0).size() > 0);
  }

  @Test
  public void shouldFilterBasedOnRowRestriction()
      throws InterruptedException, ExecutionException, TimeoutException {

    ReadSession.TableReadOptions readOptions =
        ReadSession.TableReadOptions.newBuilder().setRowRestriction("true = false").build();

    AvroByteStringDecoder um = new AvroByteStringDecoder(Col1Schema());

    List<BigQueryRecord> bigQueryRecords =
        BigQueryStorage.typed(Project(), Dataset(), Table(), DataFormat.AVRO, readOptions, um)
            .withAttributes(mockBQReader())
            .reduce(
                (a, b) -> {
                  a.addAll(b);
                  return a;
                })
            .runWith(Sink.seq(), system())
            .toCompletableFuture()
            .get()
            .get(0);

    assertEquals("number of generic records should be filtered to 0", 0, bigQueryRecords.size());
  }

  @Test
  public void shouldFilterColumns()
      throws InterruptedException, ExecutionException, TimeoutException {
    AvroSchema avroSchema = AvroSchema.newBuilder().setSchema(Col1Schema().toString()).build();
    AvroRows avroRows = recordsAsRows(Col1AvroRecord());

    AvroDecoder avroDecoder = AvroDecoder.apply(avroSchema.getSchema());

    GenericRecord genericRecord = avroDecoder.decodeRows(avroRows.serializedBinaryRows()).apply(0);
    BigQueryRecord expected = BigQueryRecord.fromAvro(genericRecord);

    ReadSession.TableReadOptions readOptions =
        ReadSession.TableReadOptions.newBuilder().addSelectedFields("col1").build();

    AvroByteStringDecoder um = new AvroByteStringDecoder(Col1Schema());

    BigQueryRecord bigQueryRecord =
        BigQueryStorage.typed(Project(), Dataset(), Table(), DataFormat.AVRO, readOptions, um)
            .withAttributes(mockBQReader())
            .runWith(Sink.seq(), system())
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .get(0)
            .get(0);

    assertEquals("fields of generic record should only include col1", expected, bigQueryRecord);
  }

  @Test
  public void shouldRestrictNumberOfStreams()
      throws InterruptedException, ExecutionException, TimeoutException {
    Integer maxStreams = 5;
    CompletionStage<Integer> numStreams =
        BigQueryStorage.create(Project(), Dataset(), Table(), DataFormat.AVRO, 5)
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
        BigQueryStorage.create(Project(), Dataset(), Table(), DataFormat.AVRO)
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
        BigQueryStorage.create("NOT A PROJECT", Dataset(), Table(), DataFormat.AVRO)
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

    Source<
            Tuple2<
                com.google.cloud.bigquery.storage.v1.stream.ReadSession.Schema,
                List<Source<ReadRowsResponse.Rows, NotUsed>>>,
            CompletionStage<NotUsed>>
        lal = BigQueryStorage.create(Project(), "NOT A DATASET", Table(), DataFormat.AVRO);
    CompletionStage<Done> result =
        BigQueryStorage.create(Project(), "NOT A DATASET", Table(), DataFormat.AVRO)
            .withAttributes(mockBQReader())
            .runWith(Sink.ignore(), system());

    try {
      result.toCompletableFuture().get(1000, TimeUnit.SECONDS);
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
        BigQueryStorage.typed(Project(), Dataset(), "NOT A TABLE", DataFormat.AVRO, null)
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
