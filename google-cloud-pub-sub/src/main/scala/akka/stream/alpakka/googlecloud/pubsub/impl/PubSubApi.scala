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
import spray.json._
import spray.json.DefaultJsonProtocol._
import spray.json.{deserializationError, DefaultJsonProtocol, JsObject, JsString, JsValue, RootJsonFormat}

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

  private implicit val pubSubMessageFormat =
    new RootJsonFormat[PubSubMessage] {
      override def read(json: JsValue): PubSubMessage = {
        val fields = json.asJsObject.fields
        PubSubMessage(
          fields.get("data").map(_.convertTo[String]),
          fields.get("attributes").map(_.convertTo[Map[String, String]]),
          fields("messageId").convertTo[String],
          fields("publishTime").convertTo[Instant]
        )
      }
      override def write(m: PubSubMessage): JsValue =
        JsObject(
          Seq(
            "messageId" -> m.messageId.toJson,
            "publishTime" -> m.publishTime.toJson
          )
          ++ m.data.map(data => "data" -> data.toJson)
          ++ m.attributes.map(attributes => "attributes" -> attributes.toJson): _*
        )
    }

  private implicit val publishMessageFormat = new RootJsonFormat[PublishMessage] {
    def read(json: JsValue): PublishMessage = {
      val data = json.asJsObject.fields("data").convertTo[String]
      val attributes = json.asJsObject.fields("attributes").convertTo[immutable.Map[String, String]]
      PublishMessage(data, attributes)
    }
    def write(m: PublishMessage): JsValue =
      JsObject(Seq("data" -> JsString(m.data)) ++ m.attributes.map(a => "attributes" -> a.toJson): _*)
  }

  private implicit val pubSubRequestFormat = new RootJsonFormat[PublishRequest] {
    def read(json: JsValue): PublishRequest =
      PublishRequest(json.asJsObject.fields("messages").convertTo[immutable.Seq[PublishMessage]])
    def write(pr: PublishRequest): JsValue = JsObject("messages" -> pr.messages.toJson)
  }
  private implicit val gcePubSubResponseFormat = new RootJsonFormat[PublishResponse] {
    def read(json: JsValue): PublishResponse =
      PublishResponse(json.asJsObject.fields("messageIds").convertTo[immutable.Seq[String]])
    def write(pr: PublishResponse): JsValue = JsObject("messageIds" -> pr.messageIds.toJson)
  }

  private implicit val receivedMessageFormat = new RootJsonFormat[ReceivedMessage] {
    def read(json: JsValue): ReceivedMessage =
      ReceivedMessage(json.asJsObject.fields("ackId").convertTo[String],
                      json.asJsObject.fields("message").convertTo[PubSubMessage])
    def write(rm: ReceivedMessage): JsValue =
      JsObject("ackId" -> rm.ackId.toJson, "message" -> rm.message.toJson)
  }
  private implicit val pubSubPullResponseFormat = new RootJsonFormat[PullResponse] {
    def read(json: JsValue): PullResponse =
      PullResponse(json.asJsObject.fields.get("receivedMessages").map(_.convertTo[immutable.Seq[ReceivedMessage]]))
    def write(pr: PullResponse): JsValue =
      pr.receivedMessages.map(rm => JsObject("receivedMessages" -> rm.toJson)).getOrElse(JsObject.empty)
  }

  private implicit val acknowledgeRequestFormat = new RootJsonFormat[AcknowledgeRequest] {
    def read(json: JsValue): AcknowledgeRequest =
      AcknowledgeRequest(json.asJsObject.fields("ackIds").convertTo[immutable.Seq[String]]: _*)
    def write(ar: AcknowledgeRequest): JsValue = JsObject("ackIds" -> ar.ackIds.toJson)
  }
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
