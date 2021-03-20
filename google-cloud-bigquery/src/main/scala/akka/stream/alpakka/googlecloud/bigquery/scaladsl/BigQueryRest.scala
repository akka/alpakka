/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import akka.actor.ClassicActorSystemProvider
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, FromResponseUnmarshaller, Unmarshaller}
import akka.stream.alpakka.google.http.GoogleHttp
import akka.stream.alpakka.google.scaladsl.Paginated
import akka.stream.alpakka.google.{GoogleAttributes, GoogleSettings, PaginatedRequest}
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}

import scala.concurrent.Future

private[scaladsl] trait BigQueryRest {

  /**
   * Makes a single authenticated request without retries.
   *
   * @param request the [[akka.http.scaladsl.model.HttpRequest]] to make
   * @return a [[scala.concurrent.Future]] containing the [[akka.http.scaladsl.model.HttpResponse]]
   */
  def singleRequest(request: HttpRequest)(implicit system: ClassicActorSystemProvider,
                                          settings: GoogleSettings): Future[HttpResponse] =
    GoogleHttp().singleAuthenticatedRequest[HttpResponse](request)

  /**
   * Makes a series of authenticated requests to page through a resource.
   *
   * @param request the [[akka.http.scaladsl.model.HttpRequest]] to make; must be a GET request
   * @tparam Out the data model for each page of the resource
   * @return a [[akka.stream.scaladsl.Source]] that emits an [[Out]] for each page of the resource
   */
  def paginatedRequest[Out: FromResponseUnmarshaller: Paginated](request: HttpRequest): Source[Out, NotUsed] =
    PaginatedRequest[Out](request)

  // Helper methods

  protected[this] def source[Out, Mat](f: GoogleSettings => Source[Out, Mat]): Source[Out, Future[Mat]] =
    Source.fromMaterializer { (mat, attr) =>
      f(GoogleAttributes.resolveSettings(mat, attr))
    }

  protected[this] def mkFilterParam(filter: Map[String, String]): String =
    filter.view
      .map {
        case (key, value) =>
          val colonValue = if (value.isEmpty) "" else s":$value"
          s"label.$key$colonValue"
      }
      .mkString(" ")

  protected[this] implicit val doneUnmarshaller: FromEntityUnmarshaller[Done] =
    Unmarshaller.withMaterializer(_ => implicit mat => _.discardBytes().future)
}
