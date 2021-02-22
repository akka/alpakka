/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshal}
import akka.http.scaladsl.util.FastFuture
import akka.stream.alpakka.googlecloud.bigquery.impl.http.BigQueryHttp
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.Paginated
import akka.stream.alpakka.googlecloud.bigquery.{BigQueryAttributes, BigQueryException, BigQuerySettings}
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}

import scala.concurrent.Future

@InternalApi
private[bigquery] object PaginatedRequest {

  private val futureNone = Future.successful(None)

  def apply[Out: FromEntityUnmarshaller](request: HttpRequest, initialPageToken: Option[String])(
      implicit paginated: Paginated[Out]
  ): Source[Out, NotUsed] =
    Source
      .fromMaterializer { (mat, attr) =>
        implicit val system = mat.system
        implicit val settings = BigQueryAttributes.resolveSettings(attr, mat)

        Source.unfoldAsync[Either[Done, Option[String]], Out](Right(initialPageToken)) {
          case Left(Done) => futureNone

          case Right(pageToken) =>
            val updatedRequest = pageToken.fold(request)(addPageToken(request, _))
            sendAndParseRequest[Out](updatedRequest).map { out =>
              val nextPageToken = paginated
                .pageToken(out)
                .fold[Either[Done, Option[String]]](Left(Done))(pageToken => Right(Some(pageToken)))
              Some((nextPageToken, out))
            }(ExecutionContexts.parasitic)
        }
      }
      .mapMaterializedValue(_ => NotUsed)

  private def addPageToken(request: HttpRequest, pageToken: String): HttpRequest =
    request.withUri(request.uri.withQuery(Query.Cons("pageToken", pageToken, request.uri.query())))

  private def sendAndParseRequest[Out: FromEntityUnmarshaller](request: HttpRequest)(
      implicit system: ActorSystem,
      settings: BigQuerySettings
  ): Future[Out] = {
    import BigQueryException._
    import system.dispatcher
    BigQueryHttp().retryRequestWithOAuth(request).flatMap { response =>
      Unmarshal(response.entity).to[Out]
    }
  }
}
