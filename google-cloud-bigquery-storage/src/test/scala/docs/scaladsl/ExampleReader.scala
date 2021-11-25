/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.http.scaladsl.unmarshalling.FromByteStringUnmarshaller
import akka.stream.alpakka.googlecloud.bigquery.storage.{BigQueryRecord, BigQueryStorageSettings}
import akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl.{
  BigQueryArrowStorage,
  BigQueryAvroStorage,
  BigQueryStorageAttributes,
  GrpcBigQueryStorageReader
}
import org.scalatestplus.mockito.MockitoSugar.mock

//#read-all
import akka.NotUsed
import com.google.cloud.bigquery.storage.v1.storage.ReadRowsResponse
import com.google.cloud.bigquery.storage.v1.DataFormat
import com.google.cloud.bigquery.storage.v1.stream.ReadSession
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

  //#read-merged
  implicit val unmarshaller: FromByteStringUnmarshaller[List[BigQueryRecord]] =
    mock[FromByteStringUnmarshaller[List[BigQueryRecord]]]
  val sequentialSource: Source[List[BigQueryRecord], Future[NotUsed]] =
    BigQueryStorage.createMergedStreams("projectId", "datasetId", "tableId", DataFormat.AVRO)
  //#read-merged

  //#read-arrow-merged
  val arrowSequentialSource: Source[Seq[BigQueryRecord], Future[NotUsed]] =
    BigQueryArrowStorage.readRecordsMerged("projectId", "datasetId", "tableId")
  //#read-arrow-merged

  //#read-arrow-all
  val arrowParallelSource: Source[Seq[Source[BigQueryRecord, NotUsed]], Future[NotUsed]] =
    BigQueryArrowStorage.readRecords("projectId", "datasetId", "tableId")
  //#read-arrow-all

  //#read-avro-merged
  val avroSequentialSource: Source[Seq[BigQueryRecord], Future[NotUsed]] =
    BigQueryAvroStorage.readRecordsMerged("projectId", "datasetId", "tableId")
  //#read-avro-merged

  //#read-avro-all
  val avroParallelSource: Source[Seq[Source[BigQueryRecord, NotUsed]], Future[NotUsed]] =
    BigQueryAvroStorage.readRecords("projectId", "datasetId", "tableId")
  //#read-avro-all

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
