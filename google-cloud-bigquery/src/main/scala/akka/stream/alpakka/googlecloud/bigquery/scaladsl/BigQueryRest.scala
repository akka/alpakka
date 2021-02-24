/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.http.scaladsl.model.HttpMethods.GET
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import akka.stream.alpakka.googlecloud.bigquery.{BigQueryAttributes, BigQuerySettings}
import akka.stream.alpakka.googlecloud.bigquery.impl.PaginatedRequest
import akka.stream.alpakka.googlecloud.bigquery.impl.http.BigQueryHttp
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.BigQueryRest.QueryPrependOption
import akka.stream.scaladsl.Source

import scala.concurrent.Future
import scala.language.implicitConversions

private[scaladsl] trait BigQueryRest {

  /**
   * Makes a single authenticated request without retries.
   *
   * @param request the [[akka.http.scaladsl.model.HttpRequest]] to make
   * @return a [[scala.concurrent.Future]] containing the [[akka.http.scaladsl.model.HttpResponse]]
   */
  def singleRequest(request: HttpRequest)(implicit system: ClassicActorSystemProvider,
                                          settings: BigQuerySettings): Future[HttpResponse] =
    BigQueryHttp().singleRequestWithOAuth(request)

  /**
   * Makes a series of authenticated requests to page through a resource.
   *
   * @param request the [[akka.http.scaladsl.model.HttpRequest]] to make; must be a GET request
   * @tparam Out the data model for each page of the resource
   * @return a [[akka.stream.scaladsl.Source]] that emits an [[Out]] for each page of the resource
   */
  def paginatedRequest[Out: FromEntityUnmarshaller: Paginated](request: HttpRequest): Source[Out, NotUsed] = {
    val query = request.uri.query()
    val initialPageToken = query.get("pageToken")
    val queryWithoutPageToken = query.filterNot(_._1 == "pageToken")
    paginatedRequest(request, queryWithoutPageToken, initialPageToken)
  }

  protected[this] def paginatedRequest[Out: FromEntityUnmarshaller: Paginated](
      request: HttpRequest,
      query: Query,
      initialPageToken: Option[String] = None
  ): Source[Out, NotUsed] = {
    require(request.method == GET, "Paginated request must be a GET request.")
    PaginatedRequest[Out](request, query, initialPageToken)
  }

  // Helper methods

  protected[this] def source[Out, Mat](f: BigQuerySettings => Source[Out, Mat]): Source[Out, Future[Mat]] =
    Source.fromMaterializer { (mat, attr) =>
      f(BigQueryAttributes.resolveSettings(attr, mat))
    }

  protected[this] implicit def queryPrependOption(query: Query) = new QueryPrependOption(query)

  protected[this] def mkFilterParam(filter: Map[String, String]): String =
    filter.view
      .map {
        case (key, value) =>
          val colonValue = if (value.isEmpty) "" else s":$value"
          s"label.$key$colonValue"
      }
      .mkString(" ")

}

object BigQueryRest {
  private[scaladsl] final class QueryPrependOption(val query: Query) extends AnyVal {
    def ?+:(kv: (String, Option[Any])): Query = kv._2.fold(query)(v => Query.Cons(kv._1, v.toString, query))
  }
}
