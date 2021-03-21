/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google

import akka.NotUsed
import akka.annotation.InternalApi
import akka.http.scaladsl.model.HttpMethods.{POST, PUT}
import akka.http.scaladsl.model.StatusCodes.{Created, OK, PermanentRedirect}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.ByteRange.Slice
import akka.http.scaladsl.model.headers.{
  `Content-Range`,
  Location,
  ModeledCustomHeader,
  ModeledCustomHeaderCompanion,
  Range,
  RawHeader
}
import akka.http.scaladsl.unmarshalling.{FromResponseUnmarshaller, Unmarshal, Unmarshaller}
import akka.stream.Materializer
import akka.stream.alpakka.google.http.GoogleHttp
import akka.stream.alpakka.google.util.{AnnotateLast, EitherFlow, MaybeLast}
import akka.stream.scaladsl.{Flow, RetryFlow, Source}
import akka.util.ByteString

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

@InternalApi
private[alpakka] object ResumableUpload {

  final case class InvalidResponseException(override val info: ErrorInfo) extends ExceptionWithErrorInfo(info)
  final case class UploadFailedException() extends Exception
  private final case class Chunk(bytes: ByteString, position: Long)

  final case class `X-Upload-Content-Type`(value: ContentType)

  def apply[T: FromResponseUnmarshaller](request: HttpRequest): Flow[ByteString, T, NotUsed] = {

    require(request.method == POST, "Resumable upload must be initiated by POST request")
    require(
      request.headers.exists(_.lowercaseName() == "x-upload-content-type"),
      "Resumable upload must specify `X-Upload-Content-Type` header"
    )
    require(
      request.uri.rawQueryString.exists(_.contains("uploadType=resumable")),
      "Resumable upload must include query parameter `uploadType=resumable`"
    )

    Flow
      .fromMaterializer { (mat, attr) =>
        import mat.executionContext
        implicit val materializer = mat
        implicit val settings = GoogleAttributes.resolveSettings(mat, attr)
        val uploadChunkSize = settings.requestSettings.uploadChunkSize

        val in = Flow[ByteString]
          .via(chunker(uploadChunkSize))
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

        val upload: Flow[Future[Either[T, MaybeLast[Chunk]]], Try[T], Future[NotUsed]] = Flow
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

        in.via(upload).map(_.get)
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  private def initiateSession(request: HttpRequest)(implicit mat: Materializer,
                                                    settings: GoogleSettings): Future[Uri] = {
    implicit val system = mat.system
    import implicits._

    implicit val um = Unmarshaller.withMaterializer { implicit ec => implicit mat => response: HttpResponse =>
      response.discardEntityBytes().future.map { _ =>
        response.header[Location].fold(throw InvalidResponseException(ErrorInfo("No Location header")))(_.uri)
      }
    }.withDefaultRetry

    GoogleHttp().singleAuthenticatedRequest[Uri](request)
  }

  private def uploadChunk[T: FromResponseUnmarshaller](
      request: HttpRequest
  )(implicit mat: Materializer): Flow[Either[T, MaybeLast[Chunk]], Try[T], NotUsed] = {
    implicit val system = mat.system

    import implicits._
    val um = implicitly[FromResponseUnmarshaller[T]].withoutRetries

    val pool = {
      val authority = request.uri.authority
      Flow[HttpRequest]
        .map((_, ()))
        .via(GoogleHttp().cachedHostConnectionPoolWithContext(authority.host.address, authority.port)(um))
        .map(_._1)
    }

    EitherFlow(
      Flow[T].map(Success(_): Try[T]),
      Flow[MaybeLast[Chunk]].map {
        case maybeLast @ MaybeLast(Chunk(bytes, position)) =>
          val totalLength = if (maybeLast.isLast) Some(position + bytes.length) else None
          val header = `Content-Range`(ContentRange(position, position + bytes.length - 1, totalLength))
          request.addHeader(header).withEntity(bytes)
      } via pool
    ).map(_.merge).mapMaterializedValue(_ => NotUsed)
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
          .singleAuthenticatedRequest[Either[T, Long]](request.addHeader(statusRequestHeader))
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

  private def chunker(chunkSize: Int) = Flow[ByteString].map(Some(_)).concat(Source.single(None)).statefulMapConcat {
    () =>
      val chunkBuilder = ByteString.newBuilder
      bytes =>
        bytes.fold(Some(chunkBuilder.result()).filter(_.nonEmpty).toList) { bytes =>
          chunkBuilder ++= bytes
          if (chunkBuilder.length < chunkSize) {
            Nil
          } else if (chunkBuilder.length == chunkSize) {
            val chunk = chunkBuilder.result()
            chunkBuilder.clear()
            chunk :: Nil
          } else { // chunkBuilder.length > chunkSize
            val result = chunkBuilder.result()
            chunkBuilder.clear()
            val (chunk, init) = result.splitAt(chunkSize)
            chunkBuilder ++= init
            chunk :: Nil
          }
        }
  }

}

object `X-Upload-Content-Type` extends ModeledCustomHeaderCompanion[`X-Upload-Content-Type`] {
  override def name: String = "X-Upload-Content-Type"
  override def parse(value: String): Try[`X-Upload-Content-Type`] =
    ContentType
      .parse(value)
      .fold(
        errorInfos => Failure(new IllegalHeaderException(errorInfos.headOption.getOrElse(ErrorInfo()))),
        contentType => Success(`X-Upload-Content-Type`(contentType))
      )
}

final case class `X-Upload-Content-Type` private (contentType: ContentType)
    extends ModeledCustomHeader[`X-Upload-Content-Type`] {
  override def value(): String = contentType.toString()
  override def renderInRequests(): Boolean = true
  override def renderInResponses(): Boolean = false
  override def companion = `X-Upload-Content-Type`
}
