/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage.impl

import akka.annotation.InternalApi
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{FormData, HttpMethods, HttpRequest, HttpResponse, StatusCodes}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.storage.impl.GoogleTokenApi.{AccessTokenExpiry, OAuthResponse}
import pdi.jwt.{Jwt, JwtAlgorithm, JwtClaim, JwtTime}
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import java.time.Clock
import scala.concurrent.Future

@InternalApi
private[impl] class GoogleTokenApi(http: => HttpExt, settings: TokenApiSettings) {
  implicit val clock: Clock = Clock.systemUTC

  protected val encodingAlgorithm: JwtAlgorithm.RS256.type = JwtAlgorithm.RS256

  def now: Long = JwtTime.nowSeconds

  private val oneHour = 3600

  private def generateJwt(clientEmail: String, privateKey: String): String = {
    val claim =
      JwtClaim(content = s"""{"scope":"${settings.scope}","aud":"${settings.url}"}""", issuer = Option(clientEmail))
        .expiresIn(oneHour)
        .issuedNow
    Jwt.encode(claim, privateKey, encodingAlgorithm)
  }

  def getAccessToken(clientEmail: String, privateKey: String)(
      implicit materializer: Materializer
  ): Future[AccessTokenExpiry] = {
    import SprayJsonSupport._
    import materializer.executionContext

    val expiresAt = now + oneHour
    val jwt = generateJwt(clientEmail, privateKey)

    val requestEntity = FormData(
      "grant_type" -> "urn:ietf:params:oauth:grant-type:jwt-bearer",
      "assertion" -> jwt
    ).toEntity

    for {
      response <- GoogleRetry.retryingRequestToResponse(
        http,
        HttpRequest(HttpMethods.POST, settings.url, entity = requestEntity)
      )
      validatedResponse <- validateResponse(response)
      result <- Unmarshal(validatedResponse.entity).to[OAuthResponse]
    } yield {
      AccessTokenExpiry(
        accessToken = result.access_token,
        expiresAt = expiresAt
      )
    }
  }

  private def validateResponse(response: HttpResponse)(implicit mat: Materializer): Future[HttpResponse] = {
    import mat.executionContext
    response match {
      case HttpResponse(StatusCodes.ServerError(status), _, responseEntity, _) =>
        Unmarshal(responseEntity).to[String].map[HttpResponse] { body =>
          throw new RuntimeException(s"Failed to request token, got $status with body: $body")
        }
      case other => Future.successful(other)
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
