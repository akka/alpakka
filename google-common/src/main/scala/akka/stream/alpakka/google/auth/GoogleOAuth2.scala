/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.auth

import akka.annotation.InternalApi
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.HttpMethods.POST
import akka.http.scaladsl.model.{FormData, HttpRequest}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import akka.stream.alpakka.google.GoogleSettings
import akka.stream.alpakka.google.http.GoogleHttp
import pdi.jwt.JwtAlgorithm.RS256
import pdi.jwt.{JwtClaim, JwtSprayJson}
import spray.json.DefaultJsonProtocol._
import spray.json.JsonFormat

import java.time.Clock
import scala.concurrent.Future

@InternalApi
private[auth] object GoogleOAuth2 {

  private val oAuthTokenUrl = "https://oauth2.googleapis.com/token"

  def getAccessToken(clientEmail: String, privateKey: String, scopes: Seq[String])(
      implicit mat: Materializer,
      settings: GoogleSettings,
      clock: Clock
  ): Future[AccessToken] = {
    import GoogleOAuth2Exception._
    import SprayJsonSupport._
    import mat.executionContext
    implicit val system = mat.system

    val jwt = generateJwt(clientEmail, privateKey, scopes)

    val entity = FormData(
      "grant_type" -> "urn:ietf:params:oauth:grant-type:jwt-bearer",
      "assertion" -> jwt
    ).toEntity

    for {
      response <- GoogleHttp().retryRequest(HttpRequest(POST, oAuthTokenUrl, entity = entity))
      token <- Unmarshal(response.entity).to[AccessToken]
    } yield token
  }

  private def generateJwt(clientEmail: String, privateKey: String, scopes: Seq[String])(
      implicit clock: Clock
  ): String = {
    import spray.json._

    val scope = scopes.mkString(" ")
    val claim = JwtClaim(content = JwtClaimContent(scope).toJson.compactPrint,
                         audience = Some(Set(oAuthTokenUrl)),
                         issuer = Some(clientEmail))
      .expiresIn(3600)
      .issuedNow

    JwtSprayJson.encode(claim, privateKey, RS256)
  }

  final case class JwtClaimContent(scope: String)
  implicit val jwtClaimContentFormat: JsonFormat[JwtClaimContent] = jsonFormat1(JwtClaimContent)
}
