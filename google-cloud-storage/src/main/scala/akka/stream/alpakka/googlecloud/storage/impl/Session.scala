/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage.impl

import java.nio.file.{Files, Paths}
import java.security.spec.PKCS8EncodedKeySpec
import java.security.{KeyFactory, PrivateKey, Signature}
import java.time.Instant
import java.util.Base64

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.storage.GoogleAuthConfiguration
import akka.stream.alpakka.googlecloud.storage.impl.Session.{AccessTokenExpiry, OAuthResponse}
import spray.json.DefaultJsonProtocol

import scala.concurrent.Future
import spray.json._

@akka.annotation.InternalApi
private[storage] object Session {

  def apply(authConfiguration: GoogleAuthConfiguration, scopes: Seq[String]) = new Session(authConfiguration, scopes)

  final case class AccessTokenExpiry(accessToken: String, expiresAt: Long)

  private final case class OAuthResponse(access_token: String, token_type: String, expires_in: Int)

  private final object SessionProtocol extends DefaultJsonProtocol {
    implicit val oAuthResponseFormat = jsonFormat3(OAuthResponse)
  }
}

@akka.annotation.InternalApi
private[storage] final class Session(private val authConfig: GoogleAuthConfiguration, scopes: Seq[String]) {

  import Session.SessionProtocol._

  private val GoogleApisHost = "https://www.googleapis.com"
  private var maybeAccessToken: Option[Future[AccessTokenExpiry]] = None

  private def now = Instant.now()

  def getToken()(implicit as: ActorSystem, materializer: Materializer): Future[String] = {
    import materializer.executionContext
    maybeAccessToken
      .getOrElse(getNewToken())
      .flatMap { result =>
        if (expiresSoon(result)) {
          getNewToken()
        } else {
          Future.successful(result)
        }
      }
      .map(_.accessToken)
  }

  private def getNewToken()(implicit as: ActorSystem, materializer: Materializer): Future[AccessTokenExpiry] = {
    val accessToken = getAccessToken(scopes, now)
    maybeAccessToken = Some(accessToken)
    accessToken
  }

  private def expiresSoon(g: AccessTokenExpiry): Boolean = g.expiresAt < (now.getEpochSecond + 60)

  private def getAccessToken(scopes: Seq[String], when: Instant)(
      implicit as: ActorSystem,
      materializer: Materializer
  ): Future[AccessTokenExpiry] = {
    import materializer.executionContext
    val expiresAt = when.getEpochSecond + 3600
    val request = buildAuthRequest(scopes, when.getEpochSecond, expiresAt)

    val body = "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=" + request
    val ct = ContentType(MediaTypes.`application/x-www-form-urlencoded`, HttpCharsets.`UTF-8`)
    val url: Uri = s"$GoogleApisHost/oauth2/v4/token"

    for {
      response <- Http().singleRequest(HttpRequest(HttpMethods.POST, url, entity = HttpEntity(ct, body)))
      responseString <- Unmarshal(response.entity).to[String]
      result = responseString.parseJson.convertTo[OAuthResponse]
    } yield {
      AccessTokenExpiry(
        accessToken = result.access_token,
        expiresAt = expiresAt
      )
    }
  }

  /**
   * https://developers.google.com/identity/protocols/OAuth2ServiceAccount
   */
  private def buildAuthRequest(scopes: Seq[String], currentTimeSecondsUTC: Long, expiresAt: Long): String = {
    def base64(s: Array[Byte]) = new String(Base64.getUrlEncoder.encode(s))

    val header = base64("""{"alg":"RS256","typ":"JWT"}""".getBytes("UTF-8"))
    val request =
      base64(s"""
           |{
           | "iss": "${authConfig.clientEmail}",
           | "scope": "${scopes.mkString(" ")}",
           | "aud": "https://www.googleapis.com/oauth2/v4/token",
           | "exp": $expiresAt,
           | "iat": $currentTimeSecondsUTC
           |}
      """.stripMargin.getBytes("UTF-8"))

    val sign = Signature.getInstance("SHA256withRSA")
    sign.initSign(authConfig.privateKey)
    sign.update(s"$header.$request".getBytes("UTF-8"))

    val signature = base64(sign.sign())

    s"$header.$request.$signature"
  }

}
