/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.auth

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.HttpMethods.POST
import akka.http.scaladsl.model.{FormData, HttpRequest}
import akka.stream.Materializer
import akka.stream.alpakka.google.http.GoogleHttp
import akka.stream.alpakka.google.{implicits, RequestSettings}
import pdi.jwt.JwtAlgorithm.RS256
import pdi.jwt.{JwtClaim, JwtSprayJson}
import spray.json.DefaultJsonProtocol._
import spray.json.JsonFormat

import java.time.Clock
import scala.concurrent.Future
import scala.util.control.NonFatal

@InternalApi
private[auth] object GoogleOAuth2 {

  private val oAuthTokenUrl = "https://oauth2.googleapis.com/token"

  def getAccessToken(clientEmail: String, privateKey: String, scopes: Seq[String])(
      implicit mat: Materializer,
      settings: RequestSettings,
      clock: Clock
  ): Future[AccessToken] = {
    import GoogleOAuth2Exception._
    import SprayJsonSupport._
    import implicits._
    implicit val system: ActorSystem = mat.system

    try {
      val entity = FormData(
        "grant_type" -> "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion" -> generateJwt(clientEmail, privateKey, scopes)
      ).toEntity

      GoogleHttp().singleRequest[AccessToken](HttpRequest(POST, oAuthTokenUrl, entity = entity))
    } catch {
      case NonFatal(e) =>
        Future.failed(e)
    }
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
