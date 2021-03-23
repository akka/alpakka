/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.scaladsl

import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.unmarshalling.FromResponseUnmarshaller
import akka.stream.alpakka.google.http.GoogleHttp
import akka.stream.alpakka.google.{GoogleSettings, PaginatedRequest, ResumableUpload}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future

object Google extends Google

private[alpakka] trait Google {

  /**
   * Makes a request and returns the unmarshalled response. Authentication is handled automatically.
   * Retries the request if the unmarshaller throws a [[akka.stream.alpakka.google.util.Retry]].
   *
   * @param request the [[akka.http.scaladsl.model.HttpRequest]] to make
   * @tparam T the data model for the resource
   * @return a [[scala.concurrent.Future]] containing the unmarshalled response
   */
  final def singleRequest[T: FromResponseUnmarshaller](
      request: HttpRequest
  )(implicit system: ClassicActorSystemProvider, settings: GoogleSettings): Future[T] =
    GoogleHttp().singleAuthenticatedRequest[T](request)

  /**
   * Makes a series of requests to page through a resource. Authentication is handled automatically.
   * Requests are retried if the unmarshaller throws a [[akka.stream.alpakka.google.util.Retry]].
   *
   * @param request the initial [[akka.http.scaladsl.model.HttpRequest]] to make; must be a `GET` request
   * @tparam Out the data model for each page of the resource
   * @return a [[akka.stream.scaladsl.Source]] that emits an `Out` for each page of the resource
   */
  final def paginatedRequest[Out: Paginated: FromResponseUnmarshaller](request: HttpRequest): Source[Out, NotUsed] =
    PaginatedRequest[Out](request)

  /**
   * Makes a series of requests to upload a stream of bytes to a media endpoint. Authentication is handled automatically.
   * If the unmarshaller throws a [[akka.stream.alpakka.google.util.Retry]] the upload will attempt to recover and continue.
   *
   * @param request the [[akka.http.scaladsl.model.HttpRequest]] to initiate the upload; must be a `POST` request with query `uploadType=resumable`
   *                and optionally a [[akka.stream.alpakka.google.scaladsl.`X-Upload-Content-Type`]] header
   * @tparam Out the data model for the resource
   * @return a [[akka.stream.scaladsl.Sink]] that materializes a [[scala.concurrent.Future]] containing the unmarshalled resource
   */
  final def resumableUpload[Out: FromResponseUnmarshaller](request: HttpRequest): Sink[ByteString, Future[Out]] =
    ResumableUpload[Out](request)

}
