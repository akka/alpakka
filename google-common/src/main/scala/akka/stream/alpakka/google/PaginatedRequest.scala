/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, FromResponseUnmarshaller, Unmarshal}
import akka.stream.alpakka.google.http.GoogleHttp
import akka.stream.alpakka.google.scaladsl.Paginated
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}

import scala.concurrent.Future

@InternalApi
private[alpakka] object PaginatedRequest {

  private val futureNone = Future.successful(None)

  def apply[Out: FromEntityUnmarshaller](request: HttpRequest, query: Query, initialPageToken: Option[String])(
      implicit paginated: Paginated[Out],
      exceptionUnmarshaller: FromResponseUnmarshaller[Exception]
  ): Source[Out, NotUsed] =
    Source
      .fromMaterializer { (mat, attr) =>
        implicit val system = mat.system
        implicit val settings = GoogleAttributes.resolveSettings(attr, mat)

        val requestWithPageToken = addPageToken(request, query)
        Source.unfoldAsync[Either[Done, Option[String]], Out](Right(initialPageToken)) {
          case Left(Done) => futureNone

          case Right(pageToken) =>
            val updatedRequest = pageToken.fold(request)(requestWithPageToken)
            sendAndParseRequest[Out](updatedRequest).map { out =>
              val nextPageToken = paginated
                .pageToken(out)
                .fold[Either[Done, Option[String]]](Left(Done))(pageToken => Right(Some(pageToken)))
              Some((nextPageToken, out))
            }(ExecutionContexts.parasitic)
        }
      }
      .mapMaterializedValue(_ => NotUsed)

  private def addPageToken(request: HttpRequest, query: Query): String => HttpRequest = { pageToken =>
    request.withUri(request.uri.withQuery(Query.Cons("pageToken", pageToken, query)))
  }

  private def sendAndParseRequest[Out: FromEntityUnmarshaller](request: HttpRequest)(
      implicit system: ActorSystem,
      settings: GoogleSettings,
      exceptionUnmarshaller: FromResponseUnmarshaller[Exception]
  ): Future[Out] = {
    import system.dispatcher
    GoogleHttp().retryRequestWithOAuth(request).flatMap { response =>
      Unmarshal(response.entity).to[Out]
    }
  }
}
