/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package avroparquet;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.avroparquet.javadsl.AvroParquetSource;
import akka.stream.javadsl.Source;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;

public class Examples {

  // #init-system
  ActorSystem system = ActorSystem.create();
  ActorMaterializer materializer = ActorMaterializer.create(system);
  // #init-system

  // #init-source
  Configuration conf = new Configuration();

  ParquetReader<GenericRecord> reader =
      AvroParquetReader.<GenericRecord>builder(
              HadoopInputFile.fromPath(new Path("./test.parquet"), conf))
          .disableCompatibility()
          .build();

  Source<GenericRecord, NotUsed> source = AvroParquetSource.create(reader);
  // #init-source

  public Examples() throws IOException {}
}
