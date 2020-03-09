/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.storage.mock

import akka.NotUsed
import akka.actor.ActorSystem
import akka.grpc.GrpcServiceException
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.{Http, HttpConnectionContext}
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.google.cloud.bigquery.storage.v1.arrow.ArrowSchema
import com.google.cloud.bigquery.storage.v1.avro.AvroSchema
import com.google.cloud.bigquery.storage.v1.storage._
import com.google.cloud.bigquery.storage.v1.stream._
import io.grpc.Status

import scala.concurrent.Future

class BigQueryMockServer(port: Int) extends BigQueryMockData {

  def run()(implicit sys: ActorSystem, mat: Materializer): Future[Http.ServerBinding] = {

    val service: HttpRequest => Future[HttpResponse] =
      BigQueryReadHandler(new BigQueryRead {

        /**
         * Validate the project and table names, if they are as expected,
         * return maxStreamCount streams of Avro data, or DefaultNumStreams if not provided.
         * Only supports Avro DataFormat currently
         */
        override def createReadSession(in: CreateReadSessionRequest): Future[ReadSession] = {
          val readSession = in.readSession.get
          if (in.parent != ProjectFullName) {
            val msg = s"Wrong project, should be $ProjectFullName"
            Future.failed(new GrpcServiceException(Status.INVALID_ARGUMENT.augmentDescription(msg)))
          } else if (readSession.table != TableFullName) {
            val msg = s"Wrong table, should be $TableFullName"
            Future.failed(new GrpcServiceException(Status.INVALID_ARGUMENT.augmentDescription(msg)))
          } else {
            val numStreams = if (in.maxStreamCount != 0) in.maxStreamCount else DefaultNumStreams
            val streams =
              if (readSession.readOptions.isEmpty)
                (1 to numStreams).map(id => ReadStream(s"$ReadSessionName/streams/$id"))
              else Seq.empty
            val schema =
              if (readSession.dataFormat == DataFormat.AVRO)
                ReadSession.Schema.AvroSchema(AvroSchema(Schema.toString))
              else
                ReadSession.Schema.ArrowSchema(
                  ArrowSchema(com.google.protobuf.ByteString.copyFromUtf8("NOT A REAL SCHEMA"))
                )

            Future.successful(
              readSession.copy(
                schema = schema,
                name = ReadSessionName,
                streams = streams
              )
            )
          }
        }

        /**
         * Regardless of the request, return a stream of ResponsesPerStream ReadRowsResponses,
         * each with RecordsPerReadRowsResponse Avro records
         */
        override def readRows(in: ReadRowsRequest): Source[ReadRowsResponse, NotUsed] =
          Source(1 to ResponsesPerStream).map(
            _ =>
              ReadRowsResponse(rowCount = RecordsPerReadRowsResponse,
                               rows = ReadRowsResponse.Rows.AvroRows(RecordsAsRows))
          )

        override def splitReadStream(in: SplitReadStreamRequest): Future[SplitReadStreamResponse] = ???
      })

    Http().bindAndHandleAsync(service, interface = "0.0.0.0", port = port, connectionContext = HttpConnectionContext())
  }
}
