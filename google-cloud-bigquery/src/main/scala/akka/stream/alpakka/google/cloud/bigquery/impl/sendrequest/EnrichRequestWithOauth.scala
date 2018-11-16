/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.cloud.bigquery.impl.sendrequest

import akka.NotUsed
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.Materializer
import akka.stream.alpakka.google.cloud.bigquery.impl.GoogleSession
import akka.stream.scaladsl.Flow

object EnrichRequestWithOauth {

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
