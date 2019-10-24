/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.sendrequest

import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.bigquery.impl.GoogleSession
import akka.stream.scaladsl.Flow

import scala.concurrent.{ExecutionContext, Future}

object SendRequestWithOauthHandling {

  def apply(googleSession: GoogleSession, http: HttpExt)(
      implicit mat: Materializer
  ) =
    Flow[HttpRequest]
      .via(EnrichRequestWithOauth(googleSession))
      .mapAsync(1)(http.singleRequest(_))
      .mapAsync(1)(handleRequestError(_))

  private def handleRequestError(response: HttpResponse)(implicit materializer: Materializer) = {
    implicit val ec: ExecutionContext = materializer.executionContext
    if (response.status.isFailure) {
      Unmarshal(response.entity)
        .to[String]
        .map(
          errorBody => throw new IllegalStateException(s"Unexpected error in response: ${response.status}, $errorBody")
        )
    } else {
      Future.successful(response)
    }
  }
}
