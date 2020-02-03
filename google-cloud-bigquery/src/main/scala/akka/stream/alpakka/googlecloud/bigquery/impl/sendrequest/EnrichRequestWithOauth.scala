/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.sendrequest

import akka.NotUsed
import akka.annotation.InternalApi
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.bigquery.impl.GoogleSession
import akka.stream.scaladsl.Flow

@InternalApi
private[impl] object EnrichRequestWithOauth {

  case class TokenErrorException() extends Exception

  def apply(
      googleSession: GoogleSession
  )(implicit materializer: Materializer): Flow[HttpRequest, HttpRequest, NotUsed] = {
    implicit val executionContext = materializer.executionContext
    Flow[HttpRequest].mapAsync(1) { request =>
      googleSession.getToken.map { token =>
        request.addHeader(Authorization(OAuth2BearerToken(token)))
      }
    }
  }
}
