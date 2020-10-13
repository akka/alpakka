/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.impl

import akka.annotation.InternalApi
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{FormData, HttpMethods, HttpRequest, HttpResponse, StatusCodes}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import GoogleTokenApi.{AccessTokenExpiry, OAuthResponse}
import pdi.jwt.{Jwt, JwtAlgorithm, JwtClaim, JwtTime}
import spray.json.{DefaultJsonProtocol, RootJsonFormat}
import java.time.Clock

import scala.concurrent.Future

@InternalApi
private[googlecloud] class GoogleTokenApi(http: => HttpExt) {
  implicit val clock: Clock = Clock.systemUTC()

  protected val encodingAlgorithm: JwtAlgorithm.RS256.type = JwtAlgorithm.RS256

  private val googleTokenUrl = "https://www.googleapis.com/oauth2/v4/token"
  private val scope = "https://www.googleapis.com/auth/pubsub"

  def now: Long = JwtTime.nowSeconds
  private val oneHour = 3600

  private def generateJwt(clientEmail: String, privateKey: String): String = {
    val claim = JwtClaim(content = s"""{"scope":"$scope","aud":"$googleTokenUrl"}""", issuer = Option(clientEmail))
      .expiresIn(oneHour)
      .issuedNow
    Jwt.encode(claim, privateKey, encodingAlgorithm)
  }

  def getAccessToken(clientEmail: String, privateKey: String)(implicit
      materializer: Materializer
  ): Future[AccessTokenExpiry] = {
    import materializer.executionContext

    val expiresAt = now + oneHour
    val jwt = generateJwt(clientEmail, privateKey)

    val requestEntity = FormData(
      "grant_type" -> "urn:ietf:params:oauth:grant-type:jwt-bearer",
      "assertion" -> jwt
    ).toEntity

    for {
      response <- GoogleRetry.singleRequest(
        http,
        HttpRequest(HttpMethods.POST, googleTokenUrl, entity = requestEntity)
      )
      result <- readResponse(response)
    } yield {
      AccessTokenExpiry(
        accessToken = result.access_token,
        expiresAt = expiresAt
      )
    }
  }

  private def readResponse(response: HttpResponse)(implicit mat: Materializer): Future[OAuthResponse] = {
    import SprayJsonSupport._
    import mat.executionContext
    response match {
      case HttpResponse(StatusCodes.OK, _, responseEntity, _) =>
        Unmarshal(responseEntity).to[OAuthResponse]
      case HttpResponse(status, _, responseEntity, _) =>
        Unmarshal(responseEntity).to[String].map[OAuthResponse] { body =>
          throw new RuntimeException(s"Request failed for POST $googleTokenUrl, got $status with body: $body")
        }
    }
  }
}

@InternalApi
private[googlecloud] object GoogleTokenApi {
  case class AccessTokenExpiry(accessToken: String, expiresAt: Long)
  case class OAuthResponse(access_token: String, token_type: String, expires_in: Int)

  import DefaultJsonProtocol._
  implicit val oAuthResponseJsonFormat: RootJsonFormat[OAuthResponse] = jsonFormat3(OAuthResponse)
}
