/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.model.HttpMethods.GET
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.unmarshalling.FromResponseUnmarshaller
import akka.stream.alpakka.google.http.GoogleHttp
import akka.stream.alpakka.google.scaladsl.Paginated
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}

import scala.concurrent.Future

@InternalApi
private[alpakka] object PaginatedRequest {

  private val futureNone = Future.successful(None)

  /**
   * Makes a series of authenticated requests to page through a resource.
   * Requests are retried if the unmarshaller throws a [[akka.stream.alpakka.google.util.Retry]].
   *
   * @param request the [[HttpRequest]] to make; must be a GET request
   * @tparam Out the data model for each page of the resource
   * @return a [[akka.stream.scaladsl.Source]] that emits an `Out` for each page of the resource
   */
  def apply[Out: FromResponseUnmarshaller](request: HttpRequest)(
      implicit paginated: Paginated[Out]
  ): Source[Out, NotUsed] = {

    require(request.method == GET, "Paginated request must be GET request")

    val parsedQuery = request.uri.query()
    val initialPageToken = parsedQuery.get("pageToken")
    val query = parsedQuery.filterNot(_._1 == "pageToken")

    Source
      .fromMaterializer { (mat, attr) =>
        implicit val system: ActorSystem = mat.system
        implicit val settings: GoogleSettings = GoogleAttributes.resolveSettings(mat, attr)

        val requestWithPageToken = addPageToken(request, query)
        Source.unfoldAsync[Either[Done, Option[String]], Out](Right(initialPageToken)) {
          case Left(Done) => futureNone

          case Right(pageToken) =>
            val updatedRequest = pageToken.fold(request)(requestWithPageToken)
            GoogleHttp()
              .singleAuthenticatedRequest(updatedRequest)
              .map { out =>
                val nextPageToken = paginated
                  .pageToken(out)
                  .fold[Either[Done, Option[String]]](Left(Done))(pageToken => Right(Some(pageToken)))
                Some((nextPageToken, out))
              }(ExecutionContexts.parasitic)
        }
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  private def addPageToken(request: HttpRequest, query: Query): String => HttpRequest = { pageToken =>
    request.withUri(request.uri.withQuery(Query.Cons("pageToken", pageToken, query)))
  }
}
