/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.javadsl;

import akka.stream.Attributes;
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryRecord;
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryStorageSettings;
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryStorageSpecBase;
import akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl.BigQueryStorageAttributes;
import akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl.GrpcBigQueryStorageReader;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Sink;

import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;

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
  public void filterResultsBasedOnRowRestrictionConfigured()
      throws InterruptedException, ExecutionException, TimeoutException {
    AvroByteStringDecoder um = new AvroByteStringDecoder(FullAvroSchema());

    CompletionStage<List<List<BigQueryRecord>>> bigQueryRecords =
        BigQueryStorage.createMergedStreams(
                Project(),
                Dataset(),
                Table(),
                DataFormat.AVRO,
                ReadSession.TableReadOptions.newBuilder().setRowRestriction("true = false").build(),
                um)
            .withAttributes(mockBQReader())
            .runWith(Sink.seq(), system());

    assertTrue(
        "number of generic records should be more than 0",
        bigQueryRecords.toCompletableFuture().get(5, TimeUnit.SECONDS).isEmpty());
  }

  public Attributes mockBQReader() {
    return mockBQReader(bqHost(), bqPort());
  }

  public Attributes mockBQReader(String host, int port) {
    GrpcBigQueryStorageReader reader =
        GrpcBigQueryStorageReader.apply(BigQueryStorageSettings.create(host, port), system());
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
