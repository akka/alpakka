/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.alpakka.avroparquet.javadsl.AvroParquetFlow;
import akka.stream.alpakka.avroparquet.javadsl.AvroParquetSource;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import java.io.IOException;
// #init-reader
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.avro.Schema;
import akka.stream.javadsl.Source;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
// #init-reader

public class Examples {

  private final Schema schema =
      new Schema.Parser()
          .parse(
              "{\"type\":\"record\",\"name\":\"Document\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"body\",\"type\":\"string\"}]}");
  // #init-system
  ActorSystem system = ActorSystem.create();
  // #init-system

  // #init-reader

  Configuration conf = new Configuration();

  ParquetReader<GenericRecord> reader =
      AvroParquetReader.<GenericRecord>builder(
              HadoopInputFile.fromPath(new Path("./test.parquet"), conf))
          .disableCompatibility()
          .build();
  // #init-reader

  // #init-source
  Source<GenericRecord, NotUsed> source = AvroParquetSource.create(reader);
  // #init-source

  public Examples() throws IOException {

    // #init-flow
    ParquetWriter<GenericRecord> writer =
        AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(new Path("./test.parquet"), conf))
            .withConf(conf)
            .withSchema(schema)
            .build();

    Flow<GenericRecord, GenericRecord, NotUsed> flow = AvroParquetFlow.create(writer);

    source.via(flow).runWith(Sink.ignore(), system);
    // #init-flow

  }
}
