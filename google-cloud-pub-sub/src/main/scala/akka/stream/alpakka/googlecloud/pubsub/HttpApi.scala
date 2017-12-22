/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub

import java.security.{PrivateKey, Signature}
import java.time.Instant
import java.util.Base64

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import spray.json.DefaultJsonProtocol
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import SprayJsonSupport._
import DefaultJsonProtocol._
import akka.http.scaladsl.marshalling.Marshal

import scala.concurrent.Future
import scala.collection.immutable

/**
 * https://cloud.google.com/pubsub/docs/reference/rest/
 */
@akka.annotation.InternalApi
private object HttpApi extends HttpApi {
  val DefaultPubSubGoogleApisHost = "https://pubsub.googleapis.com"
  val DefaultGoogleApisHost = "https://www.googleapis.com"
  val PubSubEmulatorHostVarName = "PUBSUB_EMULATOR_HOST"

  val PubSubGoogleApisHost: String = PubSubEmulatorHost.getOrElse(DefaultPubSubGoogleApisHost)
  val GoogleApisHost: String = PubSubEmulatorHost.getOrElse(DefaultGoogleApisHost)

  override def isEmulated = PubSubEmulatorHost.nonEmpty

  private[pubsub] lazy val PubSubEmulatorHost: Option[String] = sys.props
    .get(PubSubEmulatorHostVarName)
    .orElse(sys.env.get(PubSubEmulatorHostVarName))
    .map(host => s"http://$host")

  private case class PullRequest(returnImmediately: Boolean, maxMessages: Int)
}

@akka.annotation.InternalApi
private trait HttpApi {
  def PubSubGoogleApisHost: String
  def GoogleApisHost: String
  def isEmulated: Boolean

  private implicit val pubSubMessageFormat = DefaultJsonProtocol.jsonFormat2(PubSubMessage)
  private implicit val pubSubRequestFormat = DefaultJsonProtocol.jsonFormat1(PublishRequest.apply)
  private implicit val gcePubSubResponseFormat = DefaultJsonProtocol.jsonFormat1(PublishResponse)
  private implicit val googleOAuthResponseFormat = DefaultJsonProtocol.jsonFormat3(OAuthResponse)
  private implicit val receivedMessageFormat = DefaultJsonProtocol.jsonFormat2(ReceivedMessage)
  private implicit val pubSubPullResponseFormat = DefaultJsonProtocol.jsonFormat1(PullResponse)
  private implicit val acknowledgeRequestFormat =
    DefaultJsonProtocol.jsonFormat1(AcknowledgeRequest.apply)
  private implicit val pullRequestFormat = DefaultJsonProtocol.jsonFormat2(HttpApi.PullRequest)

  def pull(project: String, subscription: String, accessToken: String, apiKey: String)(
      implicit as: ActorSystem,
      materializer: Materializer
  ): Future[PullResponse] = {
    import materializer.executionContext

    val uri: Uri = s"$PubSubGoogleApisHost/v1/projects/$project/subscriptions/$subscription:pull?key=$apiKey"

    val request = HttpApi.PullRequest(returnImmediately = true, maxMessages = 1000)

    for {
      request <- Marshal((HttpMethods.POST, uri, request)).to[HttpRequest]
      response <- Http().singleRequest(request.addCredentials(OAuth2BearerToken(accessToken)))
      pullResponse <- Unmarshal(response).to[PullResponse]
    } yield pullResponse
  }

  def acknowledge(project: String,
                  subscription: String,
                  maybeAccessToken: Option[String],
                  apiKey: String,
                  request: AcknowledgeRequest)(implicit as: ActorSystem, materializer: Materializer): Future[Unit] = {
    import materializer.executionContext

    val url: Uri =
      s"$PubSubGoogleApisHost/v1/projects/$project/subscriptions/$subscription:acknowledge?key=$apiKey"

    for {
      request <- Marshal((HttpMethods.POST, url, request)).to[HttpRequest]
      response <- doRequest(request, maybeAccessToken)
    } yield {
      response.discardEntityBytes()
      if (response.status.isSuccess()) {
        ()
      } else {
        new RuntimeException("unexpected response acknowledging messages " + response)
      }
    }
  }

  private[this] def doRequest(request: HttpRequest, maybeAccessToken: Option[String])(implicit as: ActorSystem) =
    Http().singleRequest(
      maybeAccessToken.map(accessToken => request.addCredentials(OAuth2BearerToken(accessToken))).getOrElse(request)
    )

  def publish(project: String,
              topic: String,
              maybeAccessToken: Option[String],
              apiKey: String,
              request: PublishRequest)(
      implicit as: ActorSystem,
      materializer: Materializer
  ): Future[immutable.Seq[String]] = {
    import materializer.executionContext

    val url: Uri = s"$PubSubGoogleApisHost/v1/projects/$project/topics/$topic:publish"

    for {
      request <- Marshal((HttpMethods.POST, url, request)).to[HttpRequest]
      response <- doRequest(request, maybeAccessToken)
      publishResponse <- Unmarshal(response.entity).to[PublishResponse]
    } yield publishResponse.messageIds
  }

  def getAccessToken(clientEmail: String, privateKey: PrivateKey, when: Instant)(
      implicit as: ActorSystem,
      materializer: Materializer
  ): Future[AccessTokenExpiry] = {
    import materializer.executionContext
    val expiresAt = when.getEpochSecond + 3600
    val request = buildAuthRequest(clientEmail, when.getEpochSecond, expiresAt, privateKey)

    val body = "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=" + request
    val ct = ContentType(MediaTypes.`application/x-www-form-urlencoded`, HttpCharsets.`UTF-8`)
    val url: Uri = s"$GoogleApisHost/oauth2/v4/token"

    for {
      response <- Http().singleRequest(HttpRequest(HttpMethods.POST, url, entity = HttpEntity(ct, body)))
      result <- Unmarshal(response.entity).to[OAuthResponse]
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
