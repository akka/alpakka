/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
// #read-all
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryStorageSettings;
import akka.stream.alpakka.googlecloud.bigquery.storage.javadsl.BigQueryStorageAttributes;
import akka.stream.alpakka.googlecloud.bigquery.storage.javadsl.GoogleBigQueryStorage;
import akka.stream.alpakka.googlecloud.bigquery.storage.javadsl.GrpcBigQueryStorageReader;
import akka.stream.javadsl.Source;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions;
import org.apache.avro.generic.GenericRecord;

import java.util.concurrent.CompletionStage;
// #read-all

public class ExampleReader {

  static final ActorSystem sys = ActorSystem.create("ExampleReader");
  static final ActorMaterializer mat = ActorMaterializer.create(sys);

  // #read-all
  Source<Source<GenericRecord, NotUsed>, CompletionStage<NotUsed>> sourceOfSources =
      GoogleBigQueryStorage.read("projectId", "datasetId", "tableId");
  // #read-all

  // #read-options
  TableReadOptions readOptions =
      TableReadOptions.newBuilder()
          .setSelectedFields(0, "stringField")
          .setSelectedFields(1, "intField")
          .setRowRestriction("intField >= 5")
          .build();
  Source<Source<GenericRecord, NotUsed>, CompletionStage<NotUsed>> sourceOfSourcesFiltered =
      GoogleBigQueryStorage.read("projectId", "datasetId", "tableId", readOptions);
  // #read-options

  // #read-sequential
  Source<GenericRecord, CompletionStage<NotUsed>> sequentialSource =
      GoogleBigQueryStorage.read("projectId", "datasetId", "tableId").flatMapConcat(s -> s);
  // #read-sequential

  // #read-parallel
  Integer readParallelism = 10;
  Source<GenericRecord, CompletionStage<NotUsed>> parallelSource =
      GoogleBigQueryStorage.read("projectId", "datasetId", "tableId")
          .flatMapMerge(readParallelism, s -> s);
  // #read-parallel

  // #attributes
  GrpcBigQueryStorageReader reader =
      GrpcBigQueryStorageReader.create(BigQueryStorageSettings.apply("localhost", 8000), sys);
  Source<Source<GenericRecord, NotUsed>, CompletionStage<NotUsed>> sourceForReader =
      GoogleBigQueryStorage.read("projectId", "datasetId", "tableId")
          .withAttributes(BigQueryStorageAttributes.reader(reader));
  // #attributes
}
