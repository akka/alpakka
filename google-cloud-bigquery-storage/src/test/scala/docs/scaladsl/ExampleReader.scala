/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.googlecloud.bigquery.storage.BigQueryStorageSettings
import akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl.{BigQueryStorageAttributes, GrpcBigQueryStorageReader}
//#read-all
import akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl.BigQueryStorage
import akka.stream.scaladsl.Source
import com.google.cloud.bigquery.storage.v1.stream.ReadSession.TableReadOptions
import org.apache.avro.generic.GenericRecord

import scala.concurrent.Future
//#read-all

class ExampleReader {

  implicit val sys = ActorSystem("ExampleReader")
  implicit val mat = ActorMaterializer()

  //#read-all
  val sourceOfSources: Source[Source[GenericRecord, NotUsed], Future[NotUsed]] =
    BigQueryStorage.readAvroOnly("projectId", "datasetId", "tableId")
  //#read-all

  //#read-options
  val readOptions = TableReadOptions(selectedFields = Seq("stringField", "intField"), rowRestriction = "intField >= 5")
  val sourceOfSourcesFiltered: Source[Source[GenericRecord, NotUsed], Future[NotUsed]] =
    BigQueryStorage.readAvroOnly("projectId", "datasetId", "tableId", Some(readOptions))
  //#read-options

  //#read-sequential
  val sequentialSource: Source[GenericRecord, Future[NotUsed]] =
    BigQueryStorage
      .readAvroOnly("projectId", "datasetId", "tableId")
      .flatMapConcat(identity)
  //#read-sequential

  //#read-parallel
  val readParallelism = 10
  val parallelSource: Source[GenericRecord, Future[NotUsed]] =
    BigQueryStorage
      .readAvroOnly("projectId", "datasetId", "tableId")
      .flatMapMerge(readParallelism, identity)
  //#read-parallel

  //#attributes
  val reader: GrpcBigQueryStorageReader = GrpcBigQueryStorageReader(BigQueryStorageSettings("localhost", 8000))
  val sourceForReader: Source[Source[GenericRecord, NotUsed], Future[NotUsed]] =
    BigQueryStorage
      .readAvroOnly("projectId", "datasetId", "tableId")
      .withAttributes(
        BigQueryStorageAttributes.reader(reader)
      )
  //#attributes

}
