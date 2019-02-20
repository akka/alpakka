/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.impl

import java.time.Instant

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.pubsub._
import spray.json.DefaultJsonProtocol._
import spray.json.{deserializationError, DefaultJsonProtocol, JsString, JsValue, RootJsonFormat}

import scala.collection.immutable
import scala.concurrent.Future

/**
 * https://cloud.google.com/pubsub/docs/reference/rest/
 */
@InternalApi
private[pubsub] object PubSubApi extends PubSubApi {
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

@InternalApi
private[pubsub] trait PubSubApi {
  def PubSubGoogleApisHost: String
  def GoogleApisHost: String
  def isEmulated: Boolean

  private implicit val instantFormat = new RootJsonFormat[Instant] {
    override def read(jsValue: JsValue): Instant = jsValue match {
      case JsString(time) => Instant.parse(time)
      case _ => deserializationError("Instant required as a string of RFC3339 UTC Zulu format.")
    }
    override def write(instant: Instant): JsValue = JsString(instant.toString)
  }
  private implicit val pubSubMessageFormat = DefaultJsonProtocol.jsonFormat4(PubSubMessage.apply)
  private implicit val pubSubRequestFormat = DefaultJsonProtocol.jsonFormat1(PublishRequest.apply)
  private implicit val gcePubSubResponseFormat = DefaultJsonProtocol.jsonFormat1(PublishResponse)
  private implicit val receivedMessageFormat = DefaultJsonProtocol.jsonFormat2(ReceivedMessage)
  private implicit val pubSubPullResponseFormat = DefaultJsonProtocol.jsonFormat1(PullResponse)
  private implicit val acknowledgeRequestFormat =
    DefaultJsonProtocol.jsonFormat1(AcknowledgeRequest.apply)
  private implicit val pullRequestFormat = DefaultJsonProtocol.jsonFormat2(PubSubApi.PullRequest)

  def pull(project: String,
           subscription: String,
           maybeAccessToken: Option[String],
           returnImmediately: Boolean,
           maxMessages: Int)(
      implicit as: ActorSystem,
      materializer: Materializer
  ): Future[PullResponse] = {
    import materializer.executionContext

    val uri: Uri = s"$PubSubGoogleApisHost/v1/projects/$project/subscriptions/$subscription:pull"

    val request = PubSubApi.PullRequest(returnImmediately = returnImmediately, maxMessages = maxMessages)

    for {
      request <- Marshal((HttpMethods.POST, uri, request)).to[HttpRequest]
      response <- doRequest(request, maybeAccessToken)
      pullResponse <- Unmarshal(response).to[PullResponse]
    } yield pullResponse
  }

  def acknowledge(project: String, subscription: String, maybeAccessToken: Option[String], request: AcknowledgeRequest)(
      implicit as: ActorSystem,
      materializer: Materializer
  ): Future[Unit] = {
    import materializer.executionContext

    val url: Uri =
      s"$PubSubGoogleApisHost/v1/projects/$project/subscriptions/$subscription:acknowledge"

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

  def publish(project: String, topic: String, maybeAccessToken: Option[String], request: PublishRequest)(
      implicit as: ActorSystem,
      materializer: Materializer
  ): Future[immutable.Seq[String]] = {
    import materializer.executionContext

    val url: Uri = s"$PubSubGoogleApisHost/v1/projects/$project/topics/$topic:publish"

    for {
      request <- Marshal((HttpMethods.POST, url, request)).to[HttpRequest]
      response <- doRequest(request, maybeAccessToken)
    } yield {
      response.status match {
        case StatusCodes.OK => Unmarshal(response.entity).to[PublishResponse].map(_.messageIds)
        case _ => throw new RuntimeException(s"Unexpected publish response. Code: [${response.status}]")
      }
    }
  }.flatMap(identity)(materializer.executionContext)
}
