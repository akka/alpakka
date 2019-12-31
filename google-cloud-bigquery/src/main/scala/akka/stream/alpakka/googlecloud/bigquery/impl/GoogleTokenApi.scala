/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.http.scaladsl.{ClientTransport, HttpExt}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.headers.BasicHttpCredentials
import akka.http.scaladsl.model.{FormData, HttpMethods, HttpRequest}
import akka.http.scaladsl.settings.{ClientConnectionSettings, ConnectionPoolSettings}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.bigquery.ForwardProxy
import akka.stream.alpakka.googlecloud.bigquery.impl.GoogleTokenApi.{AccessTokenExpiry, OAuthResponse}
import pdi.jwt.{Jwt, JwtAlgorithm, JwtClaim, JwtTime}
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.concurrent.Future

@InternalApi
private[impl] class GoogleTokenApi(http: => HttpExt, system: ActorSystem, forwardProxy: Option[ForwardProxy]) {
  protected val encodingAlgorithm: JwtAlgorithm.RS256.type = JwtAlgorithm.RS256

  private val googleTokenUrl = "https://www.googleapis.com/oauth2/v4/token"
  private val scope = "https://www.googleapis.com/auth/bigquery"

  def now: Long = JwtTime.nowSeconds

  private def generateJwt(clientEmail: String, privateKey: String): String = {
    val claim = JwtClaim(content = s"""{"scope":"$scope","aud":"$googleTokenUrl"}""", issuer = Option(clientEmail))
      .expiresIn(3600)
      .issuedNow
    Jwt.encode(claim, privateKey, encodingAlgorithm)
  }

  def getAccessToken(clientEmail: String, privateKey: String)(
      implicit materializer: Materializer
  ): Future[AccessTokenExpiry] = {
    import materializer.executionContext
    import SprayJsonSupport._

    val expiresAt = now + 3600
    val jwt = generateJwt(clientEmail, privateKey)

    val requestEntity = FormData(
      "grant_type" -> "urn:ietf:params:oauth:grant-type:jwt-bearer",
      "assertion" -> jwt
    ).toEntity

    for {
      response <-
        forwardProxy match {
          case Some(fp) => http.singleRequest(HttpRequest(HttpMethods.POST, googleTokenUrl, entity = requestEntity), settings = poolSettings(fp))
          case None => http.singleRequest(HttpRequest(HttpMethods.POST, googleTokenUrl, entity = requestEntity))
        }
      result <- Unmarshal(response.entity).to[OAuthResponse]
    } yield {
      AccessTokenExpiry(
        accessToken = result.access_token,
        expiresAt = expiresAt
      )
    }
  }

  private def poolSettings(forwardProxy: ForwardProxy) = {
    val address = InetSocketAddress.createUnresolved(forwardProxy.host, forwardProxy.port)
    val transport = forwardProxy.credentials.fold(ClientTransport.httpsProxy(address))(
      c => ClientTransport.httpsProxy(address, BasicHttpCredentials(c.username, c.password))
    )

    ConnectionPoolSettings(system)
      .withConnectionSettings(
        ClientConnectionSettings(system)
          .withTransport(transport)
      )
  }

}

@InternalApi
private[bigquery] object GoogleTokenApi {
  case class AccessTokenExpiry(accessToken: String, expiresAt: Long)
  case class OAuthResponse(access_token: String, token_type: String, expires_in: Int)

  import DefaultJsonProtocol._
  implicit val oAuthResponseJsonFormat: RootJsonFormat[OAuthResponse] = jsonFormat3(OAuthResponse)
}