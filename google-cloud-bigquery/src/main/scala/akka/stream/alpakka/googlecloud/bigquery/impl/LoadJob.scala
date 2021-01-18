/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl

import akka.NotUsed
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.model.HttpMethods.PUT
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.ByteRange.Slice
import akka.http.scaladsl.model.headers.{`Content-Range`, Location, Range, RawHeader}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshal}
import akka.http.scaladsl.util.FastFuture
import akka.http.scaladsl.util.FastFuture.EnhancedFuture
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.bigquery.BigQueryException._
import akka.stream.alpakka.googlecloud.bigquery.impl.http.BigQueryHttp
import akka.stream.alpakka.googlecloud.bigquery.impl.util.{AnnotateLast, MaybeLast}
import akka.stream.alpakka.googlecloud.bigquery.{BigQueryAttributes, BigQueryException, BigQuerySettings}
import akka.stream.scaladsl.{Flow, Keep, RetryFlow, Sink}
import akka.util.ByteString

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

@InternalApi
private[bigquery] object LoadJob {

  private final case class Chunk(bytes: ByteString, position: Long)

  def apply[Job](
      request: HttpRequest
  )(implicit unmarshaller: FromEntityUnmarshaller[Job]): Sink[ByteString, Future[Job]] =
    Sink
      .fromMaterializer { (mat, attr) =>
        import mat.executionContext
        implicit val materializer = mat
        implicit val settings = BigQueryAttributes.resolveSettings(attr, mat)
        import settings.loadJobSettings

        val in = Flow[ByteString]
        // TODO replace with new groupedWeighted stage from upstream Akka
          .groupedWeightedWithin(loadJobSettings.chunkSize, 1.day)(_.length)
          .map { bytestrings =>
            val builder = ByteString.newBuilder
            bytestrings.foreach(builder.++=)
            builder.result()
          }
          .statefulMapConcat { () =>
            var cumulativeLength = 0L
            bytes => {
              val chunk = Chunk(bytes, cumulativeLength)
              cumulativeLength += bytes.length
              chunk :: Nil
            }
          }
          .via(AnnotateLast[Chunk])
          .map(FastFuture.successful)

        val upload = Flow
          .lazyFutureFlow { () =>
            initiateSession(request).map { uri =>
              val request = HttpRequest(PUT, uri)
              val flow = Flow[Future[MaybeLast[Chunk]]].mapAsync(1)(identity).via(uploadChunk(request))

              import settings.retrySettings._
              RetryFlow.withBackoff(minBackoff, maxBackoff, randomFactor, maxRetries, flow) {
                case (_, Success(_)) => None
                case (chunk, Failure(_)) => Some(updatePosition(request, chunk))
              }
            }
          }

        in.via(upload).mapConcat(_.get.toList).toMat(Sink.last)(Keep.right)
      }
      .mapMaterializedValue(_.flatten)

  private val uploadContentTypeHeader =
    RawHeader("X-Upload-Content-Type", ContentTypes.`application/octet-stream`.value)
  private val uploadTypeQuery = Query("uploadType" -> "resumable")

  private def initiateSession(request: HttpRequest)(implicit mat: Materializer,
                                                    settings: BigQuerySettings): Future[Uri] = {
    import mat.executionContext
    implicit val system = mat.system

    val initialRequest = request.withUri(request.uri.withQuery(uploadTypeQuery)).addHeader(uploadContentTypeHeader)

    BigQueryHttp()
      .retryRequestWithOAuth(initialRequest)
      .flatMap { response =>
        response.entity.discardBytes().future.map { _ =>
          response.header[Location].get.uri
        }
      }(ExecutionContexts.parasitic)
  }

  private def uploadChunk[Job](
      request: HttpRequest
  )(implicit mat: Materializer,
    settings: BigQuerySettings,
    um: FromEntityUnmarshaller[Job]): Flow[MaybeLast[Chunk], Try[Option[Job]], NotUsed] = {
    import mat.executionContext
    implicit val system = mat.system

    Flow[MaybeLast[Chunk]].mapAsync(1) {
      case maybeLast @ MaybeLast(Chunk(bytes, position)) =>
        val totalLength = if (maybeLast.isLast) Some(position + bytes.length) else None
        val uploadRequest = request
          .addHeader(`Content-Range`(ContentRange(position, position + bytes.length - 1, totalLength)))
          .withEntity(bytes)

        BigQueryHttp()
          .singleRequestWithOAuthOrFail(uploadRequest)
          .flatMap { response =>
            if (maybeLast.isLast)
              Unmarshal(response.entity).to[Job].map(Some(_))(ExecutionContexts.parasitic)
            else
              response.discardEntityBytes().future.map(_ => None)(ExecutionContexts.parasitic)
          }
          .transform(Success(_))(ExecutionContexts.parasitic)
    }
  }

  private val statusRequestHeader = RawHeader("Content-Range", "bytes */*")

  private def updatePosition(
      request: HttpRequest,
      chunk: Future[MaybeLast[Chunk]]
  )(implicit mat: Materializer, settings: BigQuerySettings): Future[MaybeLast[Chunk]] = {
    implicit val system = mat.system
    import mat.executionContext

    val statusRequest = request.addHeader(statusRequestHeader)

    chunk.fast.flatMap { maybeLast =>
      BigQueryHttp()
        .retryRequestWithOAuth(statusRequest)
        .flatMap { response =>
          response.discardEntityBytes().future.map { _ =>
            response
              .header[Range]
              .flatMap(_.ranges.headOption)
              .map {
                case Slice(_, last) =>
                  maybeLast.map {
                    case Chunk(bytes, position) =>
                      Chunk(bytes.drop(Math.toIntExact(last + 1 - position)), last + 1)
                  }
                case _ =>
                  throw BigQueryException("Unable to resume upload job.")
              } getOrElse maybeLast
          }
        }(ExecutionContexts.parasitic)
    }(ExecutionContexts.parasitic)
  }
}
