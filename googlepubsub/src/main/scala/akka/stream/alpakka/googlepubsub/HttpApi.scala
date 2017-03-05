/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.googlepubsub

import java.security.{PrivateKey, Signature}
import java.time.Instant
import java.util.Base64

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpEntity, _}
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import spray.json.DefaultJsonProtocol
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json._
import SprayJsonSupport._
import DefaultJsonProtocol._

import scala.concurrent.{ExecutionContext, Future}

/**
 * https://cloud.google.com/pubsub/docs/reference/rest/
 */
private object HttpApi extends HttpApi {
  val PubSubGoogleApisHost = "https://pubsub.googleapis.com"
  val GoogleApisHost = "https://www.googleapis.com"
}

private trait HttpApi {
  def PubSubGoogleApisHost: String
  def GoogleApisHost: String
  private implicit val pubSubMessageFormat = DefaultJsonProtocol.jsonFormat2(PubSubMessage)
  private implicit val pubSubRequestFormat = DefaultJsonProtocol.jsonFormat1(PublishRequest)
  private implicit val gcePubSubResponseFormat = DefaultJsonProtocol.jsonFormat1(PublishResponse)
  private implicit val googleOAuthResponseFormat = DefaultJsonProtocol.jsonFormat3(OAuthResponse)
  private implicit val receivedMessageFormat = DefaultJsonProtocol.jsonFormat2(ReceivedMessage)
  private implicit val pubSubPullResponseFormat = DefaultJsonProtocol.jsonFormat1(PullResponse)
  private implicit val acknowledgeRequestFormat = DefaultJsonProtocol.jsonFormat1(AcknowledgeRequest)

  private implicit def matToEx(implicit mat: Materializer): ExecutionContext = mat.executionContext

  def pull(project: String, subscription: String, accessToken: String, apiKey: String)(
      implicit as: ActorSystem,
      materializer: Materializer): Future[PullResponse] = {

    val url: Uri = s"$PubSubGoogleApisHost/v1/projects/$project/subscriptions/$subscription:pull?key=$apiKey"

    val requestString =
      """
        |{
        | "returnImmediately" : true,
        | "maxMessages" : 1000
        |}
      """.stripMargin

    val requestHttp =
      HttpRequest(HttpMethods.POST, url, entity = HttpEntity(ContentTypes.`application/json`, requestString))
        .addCredentials(OAuth2BearerToken(accessToken))

    Http().singleRequest(requestHttp).flatMap { httpResponse =>
      Unmarshal(httpResponse.entity).to[PullResponse]
    }
  }

  def acknowledge(project: String,
                  subscription: String,
                  accessToken: String,
                  apiKey: String,
                  request: AcknowledgeRequest)(implicit as: ActorSystem, materializer: Materializer): Future[Unit] = {
    val url: Uri =
      s"$PubSubGoogleApisHost/v1/projects/$project/subscriptions/$subscription:acknowledge?key=$apiKey"
    val requestString = acknowledgeRequestFormat.write(request).compactPrint
    val requestHttp =
      HttpRequest(HttpMethods.POST, url, entity = HttpEntity(ContentTypes.`application/json`, requestString))
        .addCredentials(OAuth2BearerToken(accessToken))

    Http().singleRequest(requestHttp).flatMap { httpResponse =>
      if (httpResponse.status.isSuccess()) {
        Future.successful(())
      } else {
        Future.failed(new RuntimeException("unexpected response acknowledging messages " + httpResponse))
      }
    }
  }

  def publish(project: String, topic: String, accessToken: String, apiKey: String, request: PublishRequest)(
      implicit as: ActorSystem,
      materializer: Materializer): Future[Seq[String]] = {
    val url: Uri = s"$PubSubGoogleApisHost/v1/projects/$project/topics/$topic:publish?key=$apiKey"

    val requestString = pubSubRequestFormat.write(request).compactPrint

    val requestHttp =
      HttpRequest(HttpMethods.POST, url, entity = HttpEntity(ContentTypes.`application/json`, requestString))
        .addCredentials(OAuth2BearerToken(accessToken))

    Http().singleRequest(requestHttp).flatMap { httpResponse =>
      Unmarshal(httpResponse.entity).to[PublishResponse].map(_.messageIds)
    }
  }

  def getAccessToken(clientEmail: String, privateKey: PrivateKey, when: Instant)(
      implicit as: ActorSystem,
      materializer: Materializer): Future[AccessTokenExpiry] = {
    import materializer.executionContext
    val expiresAt = when.getEpochSecond + 3600
    val request = buildAuthRequest(clientEmail, when.getEpochSecond, expiresAt, privateKey)

    val body = "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=" + request
    val ct = ContentType(MediaTypes.`application/x-www-form-urlencoded`, HttpCharsets.`UTF-8`)

    val url: Uri = s"$GoogleApisHost/oauth2/v4/token"
    Http().singleRequest(HttpRequest(HttpMethods.POST, url, entity = HttpEntity(ct, body))).flatMap { result =>
      Unmarshal(result.entity).to[OAuthResponse].map { re =>
        AccessTokenExpiry(
          accessToken = re.access_token,
          expiresAt = expiresAt
        )
      }
    }
  }

  /**
   * https://developers.google.com/identity/protocols/OAuth2ServiceAccount
   */
  def buildAuthRequest(clientEmail: String,
                       currentTimeSecondsUTC: Long,
                       expiresAt: Long,
                       privateKey: PrivateKey): String = {
    def base64(s: Array[Byte]) =
      new String(Base64.getUrlEncoder.encode(s))

    val header = base64("""{"alg":"RS256","typ":"JWT"}""".getBytes("UTF-8"))
    val request =
      base64(s"""
                |{
                | "iss": "$clientEmail",
                | "scope": "https://www.googleapis.com/auth/pubsub",
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
}
