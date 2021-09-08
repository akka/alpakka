/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.scaladsl

import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.http.scaladsl.unmarshalling.FromByteStringUnmarshaller
import akka.stream.alpakka.googlecloud.bigquery.storage.impl.SDKClientSource
import akka.stream.scaladsl.Source
import akka.stream.{Attributes, Materializer}
import akka.util.ByteString
import com.google.cloud.bigquery.storage.v1.DataFormat
import com.google.cloud.bigquery.storage.v1.storage.{BigQueryReadClient, CreateReadSessionRequest, ReadRowsResponse}
import com.google.cloud.bigquery.storage.v1.stream.ReadSession.TableReadOptions
import com.google.cloud.bigquery.storage.v1.stream.ReadSession

import com.google.cloud.bigquery.storage.v1.stream.{DataFormat => StreamDataFormat}

import scala.concurrent.{ExecutionContextExecutor, Future}

/**
 * Google BigQuery Storage Api Akka Stream operator factory.
 */
object BigQueryStorage {

  private val RequestParamsHeader = "x-goog-request-params"

  def createMergedStreams(
      projectId: String,
      datasetId: String,
      tableId: String,
      dataFormat: DataFormat,
      readOptions: Option[TableReadOptions] = None,
      maxNumStreams: Int = 0
  ): Source[(ReadSession.Schema, ReadRowsResponse.Rows), Future[NotUsed]] =
    create(projectId, datasetId, tableId, dataFormat, readOptions, maxNumStreams)
      .map(
        s => {
          s._2.reduce((a, b) => a.merge(b)).map((s._1, _))
        }
      )
      .flatMapConcat(a => a)
      .map(a => a)

  def create(
      projectId: String,
      datasetId: String,
      tableId: String,
      dataFormat: DataFormat,
      readOptions: Option[TableReadOptions] = None,
      maxNumStreams: Int = 0
  ): Source[(ReadSession.Schema, Seq[Source[ReadRowsResponse.Rows, NotUsed]]), Future[NotUsed]] =
    Source.fromMaterializer { (mat, attr) =>
      val client = reader(mat.system, attr).client
      readSession(client, projectId, datasetId, tableId, dataFormat, readOptions, maxNumStreams)
        .map { session =>
          (session.schema, SDKClientSource.read(client, session))
        }
    }

  def typed[A](
      projectId: String,
      datasetId: String,
      tableId: String,
      dataFormat: DataFormat,
      readOptions: Option[TableReadOptions] = None,
      maxNumStreams: Int = 0,
      parallelism: Int = 1
  )(implicit um: FromByteStringUnmarshaller[A]): Source[A, Future[NotUsed]] = {
    Source.fromMaterializer { (mat, attr) =>
      {
        implicit val materializer: Materializer = mat
        implicit val executionContext: ExecutionContextExecutor = mat.system.dispatcher
        val client = reader(mat.system, attr).client
        readSession(client, projectId, datasetId, tableId, dataFormat, readOptions, maxNumStreams)
          .map { session =>
            SDKClientSource.read(client, session).map { source =>
              source
                .mapAsync(1)(resp => {
                  val bytes = if (resp.isArrowRecordBatch)
                    resp.arrowRecordBatch.get.serializedRecordBatch
                  else
                    resp.avroRows.get.serializedBinaryRows
                  um(ByteString(bytes.toByteArray))
                })
            }
          }
          .map(a => a.reduceOption((a, b) => a.merge(b)))
          .filter(a => a.isDefined)
          .flatMapConcat(a => a.get)
      }
    }
  }

  private[scaladsl] def readSession(client: BigQueryReadClient,
                                    projectId: String,
                                    datasetId: String,
                                    tableId: String,
                                    dataFormat: DataFormat,
                                    readOptions: Option[TableReadOptions] = None,
                                    maxNumStreams: Int = 0) =
    Source
      .future {
        val table = s"projects/$projectId/datasets/$datasetId/tables/$tableId"
        client
          .createReadSession()
          .addHeader(RequestParamsHeader, s"read_session.table=$table")
          .invoke(
            CreateReadSessionRequest(
              parent = s"projects/$projectId",
              Some(
                ReadSession(dataFormat = StreamDataFormat.fromValue(dataFormat.getNumber),
                            table = table,
                            readOptions = readOptions)
              ),
              maxNumStreams
            )
          )
      }

  private[scaladsl] def reader(system: ClassicActorSystemProvider, attr: Attributes) =
    attr
      .get[BigQueryStorageAttributes.BigQueryStorageReader]
      .map(_.client)
      .getOrElse(GrpcBigQueryStorageReaderExt()(system).reader)
}
