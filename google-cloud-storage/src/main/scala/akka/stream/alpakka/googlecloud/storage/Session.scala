/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

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
import akka.stream.alpakka.googlecloud.storage.Session.{AccessTokenExpiry, GoogleAuthConfiguration, OAuthResponse}
import play.api.libs.json.Json

import scala.concurrent.Future
import scala.io.Source

object Session {

  def apply(authConfiguration: GoogleAuthConfiguration, scopes: Seq[String]) = new Session(authConfiguration, scopes)

  final case class AccessTokenExpiry(accessToken: String, expiresAt: Long)
  final case class GoogleAuthConfiguration(serviceAccountFile: String)

  private final case class OAuthResponse(access_token: String, token_type: String, expires_in: Int)
}

class Session(private val authConfig: GoogleAuthConfiguration, scopes: Seq[String]) {
  private val GoogleApisHost = "https://www.googleapis.com"
  private implicit val googleOAuthResponseFormat = Json.format[OAuthResponse]
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
      result = Json.parse(responseString).validate[OAuthResponse].get
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
           | "iss": "$clientEmail",
           | "scope": "${scopes.mkString(" ")}",
           | "aud": "https://www.googleapis.com/oauth2/v4/token",
           | "exp": $expiresAt,
           | "iat": $currentTimeSecondsUTC
           |}
      """.stripMargin.getBytes("UTF-8"))

    val sign = Signature.getInstance("SHA256withRSA")
    sign.initSign(privateKey)
    sign.update(s"$header.$request".getBytes("UTF-8"))

    val signature = base64(sign.sign())

    s"$header.$request.$signature"
  }

  private lazy val serviceAccountJson = {
    val path = Paths.get(authConfig.serviceAccountFile)
    if (Files.notExists(path)) {
      throw new RuntimeException(s"Service account file missing: ${path.toAbsolutePath}")
    }
    val bufferedSource = Source.fromFile(path.toFile)
    val contentAsJson = Json.parse(bufferedSource.getLines().mkString)
    bufferedSource.close()
    contentAsJson
  }
  private lazy val privateKey: PrivateKey = {
    val pk = (serviceAccountJson \ "private_key")
      .as[String]
      .replace("-----BEGIN RSA PRIVATE KEY-----\n", "")
      .replace("-----END RSA PRIVATE KEY-----", "")
      .replace("-----BEGIN PRIVATE KEY-----\n", "")
      .replace("-----END PRIVATE KEY-----", "")
      .replaceAll(raw"\s", "")
    val kf = KeyFactory.getInstance("RSA")
    val encodedPv = Base64.getDecoder.decode(pk)
    val keySpecPv = new PKCS8EncodedKeySpec(encodedPv)
    kf.generatePrivate(keySpecPv)
  }
  private lazy val clientEmail: String = (serviceAccountJson \ "client_email").as[String]

}
