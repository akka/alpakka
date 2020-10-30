/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.Materializer;
import akka.stream.alpakka.avroparquet.javadsl.AvroParquetSink;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.javadsl.TestKit;
import com.google.common.collect.Lists;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static junit.framework.TestCase.assertEquals;

// #init-writer
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.HadoopInputFile;
// #init-writer

public class AvroParquetSinkTest {

  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private final Schema schema =
      new Schema.Parser()
          .parse(
              "{\"type\":\"record\",\"name\":\"Document\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"body\",\"type\":\"string\"}]}");
  private final Configuration conf = new Configuration();
  private final List<GenericRecord> records = new ArrayList<>();
  private ActorSystem system;
  private String folder = "target/javaTestFolder";
  private final String file = "./" + folder + "/test.parquet";

  @Before
  public void setup() {
    system = ActorSystem.create();
    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true);
    records.add(new GenericRecordBuilder(schema).set("id", "1").set("body", "body11").build());
    records.add(new GenericRecordBuilder(schema).set("id", "2").set("body", "body12").build());
    records.add(new GenericRecordBuilder(schema).set("id", "3").set("body", "body13").build());
  }

  @Test
  public void createNewParquetFile()
      throws InterruptedException, IOException, TimeoutException, ExecutionException {
    // #init-writer

    Configuration conf = new Configuration();
    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true);
    ParquetWriter<GenericRecord> writer =
        AvroParquetWriter.<GenericRecord>builder(new Path(file))
            .withConf(conf)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .withSchema(schema)
            .build();

    // #init-writer

    // #init-sink
    Sink<GenericRecord, CompletionStage<Done>> sink = AvroParquetSink.create(writer);
    // #init-sink

    CompletionStage<Done> finish = Source.from(records).runWith(sink, system);

    finish.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(records.size(), checkResponse());
  }

  private int checkResponse() throws IOException {

    Path dataFile = new Path(file);
    ParquetReader<GenericRecord> reader =
        AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(dataFile, conf))
            .disableCompatibility()
            .build();

    List<GenericRecord> expectedRecords = Lists.newArrayList();
    GenericRecord rec;
    while ((rec = reader.read()) != null) {
      expectedRecords.add(rec);
    }
    reader.close();
    return expectedRecords.size();
  }

  @After
  public void checkForStageLeaksAndDeleteCreatedFiles() {
    StreamTestKit.assertAllStagesStopped(Materializer.matFromSystem(system));
    TestKit.shutdownActorSystem(system);
    File index = new File(folder);
    index.deleteOnExit();
    String[] entries = index.list();
    if (entries != null) {
      for (String s : entries) {
        File currentFile = new File(index.getPath(), s);
        currentFile.deleteOnExit();
      }
    }
  }
}
