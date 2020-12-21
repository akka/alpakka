/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshal}
import akka.http.scaladsl.util.FastFuture
import akka.stream.alpakka.googlecloud.bigquery.impl.http.BigQueryHttp
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.PageToken
import akka.stream.alpakka.googlecloud.bigquery.{BigQueryAttributes, BigQueryException, BigQuerySettings}
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}

import scala.concurrent.Future

@InternalApi
private[bigquery] object PaginatedRequest {

  def apply[Out](request: HttpRequest, initialPageToken: Option[String])(
      implicit unmarshaller: FromEntityUnmarshaller[Out],
      pageToken: PageToken[Out]
  ): Source[Out, NotUsed] =
    Source
      .fromMaterializer { (mat, attr) =>
        implicit val system = mat.system
        implicit val settings = BigQueryAttributes.resolveSettings(attr, mat)

        Source.unfoldAsync[Either[Done, Option[String]], Out](Right(initialPageToken)) {
          case Left(Done) =>
            FastFuture.successful(None)

          case Right(currentPageToken) =>
            val updatedRequest = currentPageToken.fold(request)(addPageToken(request, _))
            sendAndParseRequest[Out](updatedRequest).map { out =>
              val nextPageToken = pageToken
                .pageToken(out)
                .fold[Either[Done, Option[String]]](Left(Done))(pageToken => Right(Some(pageToken)))
              Some((nextPageToken, out))
            }(ExecutionContexts.parasitic)
        }
      }
      .mapMaterializedValue(_ => NotUsed)

  private def addPageToken(request: HttpRequest, pageToken: String): HttpRequest =
    request.withUri(request.uri.withQuery(request.uri.query().+:("pageToken" -> pageToken)))

  private def sendAndParseRequest[Out](request: HttpRequest)(
      implicit system: ActorSystem,
      settings: BigQuerySettings,
      unmarshaller: FromEntityUnmarshaller[Out]
  ): Future[Out] = {
    import BigQueryException._
    import system.dispatcher
    BigQueryHttp().retryRequestWithOAuth(request).flatMap { response =>
      Unmarshal(response.entity).to[Out]
    }
  }
}
