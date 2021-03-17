/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google

import akka.NotUsed
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.model.HttpMethods.{POST, PUT}
import akka.http.scaladsl.model.StatusCodes.{Created, OK, PermanentRedirect}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.ByteRange.Slice
import akka.http.scaladsl.model.headers.{`Content-Range`, Location, Range, RawHeader}
import akka.http.scaladsl.unmarshalling.{FromResponseUnmarshaller, Unmarshal, Unmarshaller}
import akka.stream.Materializer
import akka.stream.alpakka.google.http.GoogleHttp
import akka.stream.alpakka.google.util.{AnnotateLast, MaybeLast}
import akka.stream.scaladsl.{Flow, RetryFlow}
import akka.util.ByteString

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

@InternalApi
private[alpakka] object ResumableUpload {

  final case class InvalidResponseException(override val info: ErrorInfo) extends ExceptionWithErrorInfo(info)
  final case class UploadFailedException() extends Exception
  private final case class Chunk(bytes: ByteString, position: Long)

  def apply[T: FromResponseUnmarshaller](
      request: HttpRequest
  ): Flow[ByteString, T, NotUsed] = {

    require(request.method == POST, "Resumable upload must be initiated by POST request")

    Flow
      .fromMaterializer { (mat, attr) =>
        import mat.executionContext
        implicit val materializer = mat
        implicit val settings = GoogleAttributes.resolveSettings(attr, mat)
        val uploadChunkSize = settings.requestSettings.uploadChunkSize

        val in = Flow[ByteString]
        // TODO replace with new groupedWeighted stage from Akka 2.6.13
          .groupedWeightedWithin(uploadChunkSize, 1.day)(_.length)
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
          .map(chunk => Future.successful(Right(chunk)))

        val upload: Flow[Future[Either[T, MaybeLast[Chunk]]], Try[Option[T]], Future[NotUsed]] = Flow
          .lazyFutureFlow { () =>
            initiateSession(request).map { uri =>
              val request = HttpRequest(PUT, uri)
              val flow = Flow[Future[Either[T, MaybeLast[Chunk]]]].mapAsync(1)(identity).via(uploadChunk(request))

              import settings.retrySettings._
              RetryFlow.withBackoff(minBackoff, maxBackoff, randomFactor, maxRetries, flow) {
                case (_, Success(_)) => None
                case (chunk, Failure(_)) => Some(updatePosition(request, chunk.map(_.right.get)))
              }
            }
          }

        in.via(upload).mapConcat(_.get.toList)
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  private val uploadContentTypeHeader =
    RawHeader("X-Upload-Content-Type", ContentTypes.`application/octet-stream`.value)
  private val uploadTypeQuery = "uploadType=resumable"
  private val `&uploadTypeQuery` = "&uploadType=resumable"

  private def initiateSession(request: HttpRequest)(implicit mat: Materializer,
                                                    settings: GoogleSettings): Future[Uri] = {
    implicit val system = mat.system
    import implicits._

    val initialRequest = request
      .withUri(
        request.uri.withRawQueryString(request.uri.rawQueryString.fold(uploadTypeQuery)(_.concat(`&uploadTypeQuery`)))
      )
      .addHeader(uploadContentTypeHeader)

    implicit val um = Unmarshaller.withMaterializer { implicit ec => implicit mat => response: HttpResponse =>
      response.discardEntityBytes().future.map { _ =>
        response.header[Location].fold(throw InvalidResponseException(ErrorInfo("No Location header")))(_.uri)
      }
    }.withDefaultRetry

    GoogleHttp().retryRequestWithOAuth[Uri](initialRequest)
  }

  private def uploadChunk[T: FromResponseUnmarshaller](
      request: HttpRequest
  )(implicit mat: Materializer,
    settings: GoogleSettings): Flow[Either[T, MaybeLast[Chunk]], Try[Option[T]], NotUsed] = {
    implicit val system = mat.system

    Flow[Either[T, MaybeLast[Chunk]]].mapAsync(1) {
      case Left(result) => Future.successful(Success(Some(result)))
      case Right(maybeLast @ MaybeLast(Chunk(bytes, position))) =>
        val totalLength = if (maybeLast.isLast) Some(position + bytes.length) else None
        val uploadRequest = request
          .addHeader(`Content-Range`(ContentRange(position, position + bytes.length - 1, totalLength)))
          .withEntity(bytes)

        implicit val um: Unmarshaller[HttpResponse, Option[T]] =
          if (maybeLast.isLast)
            implicitly[FromResponseUnmarshaller[T]].map(Some(_))
          else
            Unmarshaller.withMaterializer { implicit ec => implicit mat => response: HttpResponse =>
              response.discardEntityBytes().future.map(_ => None)
            }

        GoogleHttp()
          .singleRequestWithOAuth[Option[T]](uploadRequest)
          .transform(Success(_))(ExecutionContexts.parasitic)
    }
  }

  private val statusRequestHeader = RawHeader("Content-Range", "bytes */*")

  private def updatePosition[T: FromResponseUnmarshaller](
      request: HttpRequest,
      chunk: Future[MaybeLast[Chunk]]
  )(implicit mat: Materializer, settings: GoogleSettings): Future[Either[T, MaybeLast[Chunk]]] = {
    implicit val system = mat.system
    import implicits._

    implicit val um = Unmarshaller.withMaterializer { implicit ec => implicit mat => response: HttpResponse =>
      response.status match {
        case OK | Created => Unmarshal(response).to[T].map(Left(_))
        case PermanentRedirect =>
          response.discardEntityBytes().future.map { _ =>
            Right(
              response
                .header[Range]
                .flatMap(_.ranges.headOption)
                .collect {
                  case Slice(_, last) => last + 1
                } getOrElse 0L
            )
          }
        case _ => throw InvalidResponseException(ErrorInfo(response.status.value, response.status.defaultMessage))
      }
    }.withDefaultRetry

    import mat.executionContext
    chunk.flatMap {
      case maybeLast @ MaybeLast(Chunk(bytes, position)) =>
        GoogleHttp()
          .retryRequestWithOAuth[Either[T, Long]](request.addHeader(statusRequestHeader))
          .map {
            case Left(result) if maybeLast.isLast => Left(result)
            case Right(newPosition) if newPosition >= position =>
              Right(maybeLast.map { _ =>
                Chunk(bytes.drop(Math.toIntExact(newPosition - position)), newPosition)
              })
            case _ => throw UploadFailedException()
          }
    }
  }

}
