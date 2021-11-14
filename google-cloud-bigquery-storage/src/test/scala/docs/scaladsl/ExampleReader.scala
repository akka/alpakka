/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.unmarshalling.FromByteStringUnmarshaller
import akka.stream.alpakka.googlecloud.bigquery.storage.{BigQueryRecord, BigQueryStorageSettings}
import akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl.{BigQueryStorageAttributes, GrpcBigQueryStorageReader}
import com.google.cloud.bigquery.storage.v1.DataFormat
import com.google.cloud.bigquery.storage.v1.storage.ReadRowsResponse
import com.google.cloud.bigquery.storage.v1.stream.ReadSession
//#read-all
import akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl.BigQueryStorage
import akka.stream.scaladsl.Source
import com.google.cloud.bigquery.storage.v1.stream.ReadSession.TableReadOptions

import scala.concurrent.Future

//#read-all

class ExampleReader {

  implicit val sys = ActorSystem("ExampleReader")

  //#read-all
  val sourceOfSources: Source[(ReadSession.Schema, Seq[Source[ReadRowsResponse.Rows, NotUsed]]), Future[NotUsed]] =
    BigQueryStorage.create("projectId", "datasetId", "tableId", DataFormat.AVRO)
  //#read-all

  //#read-options
  val readOptions = TableReadOptions(selectedFields = Seq("stringField", "intField"), rowRestriction = "intField >= 5")
  val sourceOfSourcesFiltered
      : Source[(ReadSession.Schema, Seq[Source[ReadRowsResponse.Rows, NotUsed]]), Future[NotUsed]] =
    BigQueryStorage.create("projectId", "datasetId", "tableId", DataFormat.AVRO, Some(readOptions))
  //#read-options

  //#read-sequential
  implicit val unmarshaller: FromByteStringUnmarshaller[List[BigQueryRecord]] = ???
  val sequentialSource: Source[List[BigQueryRecord], Future[NotUsed]] =
    BigQueryStorage.createMergedStreams("projectId", "datasetId", "tableId", DataFormat.AVRO)
  //#read-sequential

  //#read-parallel
  val parallelSource: Source[(ReadSession.Schema, Seq[Source[ReadRowsResponse.Rows, NotUsed]]), Future[NotUsed]] =
    BigQueryStorage.create("projectId", "datasetId", "tableId", DataFormat.AVRO, Some(readOptions), 4)
  //#read-parallel

  //#attributes
  val reader: GrpcBigQueryStorageReader = GrpcBigQueryStorageReader(BigQueryStorageSettings("localhost", 8000))
  val sourceForReader: Source[(ReadSession.Schema, Seq[Source[ReadRowsResponse.Rows, NotUsed]]), Future[NotUsed]] =
    BigQueryStorage
      .create("projectId", "datasetId", "tableId", DataFormat.AVRO)
      .withAttributes(
        BigQueryStorageAttributes.reader(reader)
      )
  //#attributes

}
