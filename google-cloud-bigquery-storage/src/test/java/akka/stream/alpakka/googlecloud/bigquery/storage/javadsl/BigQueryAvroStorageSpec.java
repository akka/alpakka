/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.javadsl;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.cloud.bigquery.storage.v1.avro.AvroRows;
import com.google.cloud.bigquery.storage.v1.avro.AvroSchema;

import akka.stream.Attributes;
import akka.stream.Materializer;
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryRecord;
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryStorageSettings;
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryStorageSpecBase;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import scala.compat.java8.FutureConverters.FutureOps;

public class BigQueryAvroStorageSpec extends BigQueryStorageSpecBase {

  private final AvroSchema avroSchema = storageAvroSchema().value();
  private final AvroRows avroRows = storageAvroRows().value();

  private final Schema schema = new Schema.Parser().parse(avroSchema.schema());
  private final AvroByteStringDecoder avroByteStringDecoder = new AvroByteStringDecoder(schema);

  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private List<BigQueryRecord> expectedRecords;

  @Before
  public void setUp() throws Exception {
    ByteString byteString = ByteString.fromArray(avroRows.serializedBinaryRows().toByteArray());
    expectedRecords =
        new FutureOps<>(
                avroByteStringDecoder.apply(
                    byteString, system().dispatcher(), Materializer.matFromSystem(system())))
            .toJava()
            .toCompletableFuture()
            .get();
  }

  @Test
  public void shouldReturnResultsForAQueryInMergedRecords()
      throws ExecutionException, InterruptedException, TimeoutException {
    List<BigQueryRecord> bigQueryRecords =
        BigQueryAvroStorage.readRecordsMerged(Project(), Dataset(), Table())
            .withAttributes(mockBQReader()).runWith(Sink.seq(), system()).toCompletableFuture()
            .get(5, TimeUnit.SECONDS).stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

    assertEquals("List of records should be the same", expectedRecords, bigQueryRecords);
  }

  @Test
  public void shouldReturnResultsForAQueryInRecords()
      throws ExecutionException, InterruptedException, TimeoutException {
    List<BigQueryRecord> bigQueryRecords =
        BigQueryAvroStorage.readRecords(Project(), Dataset(), Table())
            .withAttributes(mockBQReader())
            .map(a -> a.stream().reduce(Source::merge))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .flatMapConcat(a -> a)
            .runWith(Sink.seq(), system())
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);

    assertEquals("List of records should be the same", expectedRecords, bigQueryRecords);
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
}
