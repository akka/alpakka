/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.auth

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.HttpMethods.POST
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{FormData, HttpRequest}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer

import java.time.Clock
import scala.concurrent.Future

@InternalApi
private[auth] object UserAccessMetadata {
  private val tokenUrl = "https://accounts.google.com/o/oauth2/token"
  private val `Metadata-Flavor` = RawHeader("Metadata-Flavor", "Google")

  private def tokenRequest(clientId: String, clientSecret: String, refreshToken: String): HttpRequest = {
    val entity = FormData(
      "client_id" -> clientId,
      "client_secret" -> clientSecret,
      "refresh_token" -> refreshToken,
      "grant_type" -> "refresh_token"
    ).toEntity
    HttpRequest(method = POST, uri = tokenUrl, entity = entity).addHeader(`Metadata-Flavor`)
  }

  def getAccessToken(clientId: String, clientSecret: String, refreshToken: String)(implicit
      mat: Materializer,
      clock: Clock
  ): Future[AccessToken] = {
    import SprayJsonSupport._
    import mat.executionContext
    implicit val system: ActorSystem = mat.system
    for {
      response <- Http().singleRequest(tokenRequest(clientId, clientSecret, refreshToken))
      token <- Unmarshal(response.entity).to[AccessToken]
    } yield token
  }
}
