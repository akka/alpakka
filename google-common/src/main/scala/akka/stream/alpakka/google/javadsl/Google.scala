/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.javadsl

import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.http.javadsl.model.{HttpRequest, HttpResponse}
import akka.http.javadsl.unmarshalling.Unmarshaller
import akka.http.scaladsl.{model => sm}
import akka.stream.alpakka.google.GoogleSettings
import akka.stream.alpakka.google.scaladsl.{Google => ScalaGoogle}
import akka.stream.javadsl.{Sink, Source}
import akka.util.ByteString

import java.util.concurrent.CompletionStage
import scala.compat.java8.FutureConverters._
import scala.language.implicitConversions

/**
 * Java API: Provides methods to interface with Google APIs
 */
object Google extends Google

private[alpakka] trait Google {

  /**
   * Makes a request and returns the unmarshalled response. Authentication is handled automatically.
   * Retries the request if the unmarshaller throws a [[akka.stream.alpakka.google.util.Retry]].
   *
   * @param request the [[akka.http.javadsl.model.HttpRequest]] to make
   * @tparam T the data model for the resource
   * @return a [[java.util.concurrent.CompletionStage]] containing the unmarshalled response
   */
  final def singleRequest[T](request: HttpRequest,
                             unmarshaller: Unmarshaller[HttpResponse, T],
                             settings: GoogleSettings,
                             system: ClassicActorSystemProvider): CompletionStage[T] =
    ScalaGoogle.singleRequest[T](request)(unmarshaller.asScala, system, settings).toJava

  /**
   * Makes a series of requests to page through a resource. Authentication is handled automatically.
   * Requests are retried if the unmarshaller throws a [[akka.stream.alpakka.google.util.Retry]].
   *
   * @param request the initial [[akka.http.javadsl.model.HttpRequest]] to make; must be a `GET` request
   * @tparam Out the data model for each page of the resource
   * @return a [[akka.stream.javadsl.Source]] that emits an `Out` for each page of the resource
   */
  final def paginatedRequest[Out <: Paginated](request: HttpRequest,
                                               unmarshaller: Unmarshaller[HttpResponse, Out]): Source[Out, NotUsed] = {
    implicit val um = unmarshaller.asScala
    ScalaGoogle.paginatedRequest[Out](request).asJava
  }

  /**
   * Makes a series of requests to upload a stream of bytes to a media endpoint. Authentication is handled automatically.
   * If the unmarshaller throws a [[akka.stream.alpakka.google.util.Retry]] the upload will attempt to recover and continue.
   *
   * @param request the [[akka.http.javadsl.model.HttpRequest]] to initiate the upload; must be a `POST` request with query `uploadType=resumable`
   *                and optionally a [[akka.stream.alpakka.google.javadsl.XUploadContentType]] header
   * @tparam Out the data model for the resource
   * @return a [[akka.stream.javadsl.Sink]] that materializes a [[java.util.concurrent.CompletionStage]] containing the unmarshalled resource
   */
  final def resumableUpload[Out](
      request: HttpRequest,
      unmarshaller: Unmarshaller[HttpResponse, Out]
  ): Sink[ByteString, CompletionStage[Out]] =
    ScalaGoogle.resumableUpload(request)(unmarshaller.asScala).mapMaterializedValue(_.toJava).asJava

  private implicit def requestAsScala(request: HttpRequest): sm.HttpRequest = request.asInstanceOf[sm.HttpRequest]
}
