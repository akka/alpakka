/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.impl

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.Http.HostConnectionPool
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.HttpMethods.POST
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{FromResponseUnmarshaller, Unmarshal, Unmarshaller}
import akka.stream.alpakka.google.GoogleAttributes
import akka.stream.alpakka.google.http.GoogleHttp
import akka.stream.alpakka.google.implicits._
import akka.stream.alpakka.googlecloud.pubsub._
import akka.stream.scaladsl.{Flow, FlowWithContext}
import akka.{Done, NotUsed}
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.time.Instant
import scala.collection.immutable
import scala.concurrent.Future
import scala.util.Try

/**
 * https://cloud.google.com/pubsub/docs/reference/rest/
 */
@InternalApi
private[pubsub] object PubSubApi extends PubSubApi {
  val DefaultPubSubGoogleApisHost = "pubsub.googleapis.com"
  val DefaultPubSubGoogleApisPort = 443
  val PubSubEmulatorHostVarName = "PUBSUB_EMULATOR_HOST"
  val PubSubEmulatorPortVarName = "PUBSUB_EMULATOR_PORT"

  val PubSubGoogleApisHost: String = PubSubEmulatorHost.getOrElse(DefaultPubSubGoogleApisHost)
  val PubSubGoogleApisPort: Int = PubSubEmulatorPort.getOrElse(DefaultPubSubGoogleApisPort)

  override def isEmulated = PubSubEmulatorHost.nonEmpty

  private[pubsub] lazy val PubSubEmulatorHost: Option[String] = sys.props
    .get(PubSubEmulatorHostVarName)
    .orElse(sys.env.get(PubSubEmulatorHostVarName))

  private[pubsub] lazy val PubSubEmulatorPort: Option[Int] = sys.props
    .get(PubSubEmulatorPortVarName)
    .orElse(sys.env.get(PubSubEmulatorPortVarName))
    .map(port => Int.unbox(Integer.valueOf(port)))

}

@InternalApi
private[pubsub] trait PubSubApi {
  def PubSubGoogleApisHost: String
  def PubSubGoogleApisPort: Int
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
  private implicit val pullRequestFormat = DefaultJsonProtocol.jsonFormat2(PullRequest)

  private def scheme: String = if (isEmulated) "http" else "https"

  def pull(subscription: String, returnImmediately: Boolean, maxMessages: Int): Flow[Done, PullResponse, NotUsed] =
    Flow
      .fromMaterializer { (mat, attr) =>
        import mat.executionContext
        implicit val settings = GoogleAttributes.resolveSettings(mat, attr)

        val url: Uri = Uri.from(
          scheme = scheme,
          host = PubSubGoogleApisHost,
          port = PubSubGoogleApisPort,
          path = s"/v1/projects/${settings.projectId}/subscriptions/$subscription:pull"
        )

        Flow[Done].mapAsync(1) { _ =>
          for {
            entity <- Marshal(PullRequest(returnImmediately, maxMessages)).to[RequestEntity]
            request = HttpRequest(POST, url, entity = entity)
            response <- if (isEmulated)
              GoogleHttp(mat.system).singleRequest[PullResponse](request)
            else
              GoogleHttp(mat.system).singleAuthenticatedRequest[PullResponse](request)
          } yield response
        }
      }
      .mapMaterializedValue(_ => NotUsed)

  private implicit val pullResponseUnmarshaller: FromResponseUnmarshaller[PullResponse] =
    Unmarshaller.withMaterializer { implicit ec => implicit mat => response: HttpResponse =>
      response.status match {
        case StatusCodes.Success(_) if response.entity.contentType == ContentTypes.`application/json` =>
          Unmarshal(response.entity).to[PullResponse]
        case status =>
          Unmarshal(response.entity).to[String].map { entity =>
            throw new RuntimeException(s"Unexpected pull response. Code: [$status]. Entity: [$entity]")
          }
      }
    }.withDefaultRetry

  def acknowledge(subscription: String): Flow[AcknowledgeRequest, Done, NotUsed] =
    Flow
      .fromMaterializer { (mat, attr) =>
        import mat.executionContext
        implicit val settings = GoogleAttributes.resolveSettings(mat, attr)

        val url: Uri = Uri.from(
          scheme = scheme,
          host = PubSubGoogleApisHost,
          port = PubSubGoogleApisPort,
          path = s"/v1/projects/${settings.projectId}/subscriptions/$subscription:acknowledge"
        )

        Flow[AcknowledgeRequest].mapAsync(1) { request =>
          for {
            entity <- Marshal(request).to[RequestEntity]
            request = HttpRequest(POST, url, entity = entity)
            done <- if (isEmulated)
              GoogleHttp(mat.system).singleRequest[Done](request)
            else
              GoogleHttp(mat.system).singleAuthenticatedRequest[Done](request)
          } yield done
        }
      }
      .mapMaterializedValue(_ => NotUsed)

  private implicit val acknowledgeResponseUnmarshaller: FromResponseUnmarshaller[Done] =
    Unmarshaller.withMaterializer { implicit ec => implicit mat => response: HttpResponse =>
      response.status match {
        case StatusCodes.Success(_) =>
          response.discardEntityBytes().future
        case status =>
          Unmarshal(response.entity).to[String].map { entity =>
            throw new RuntimeException(
              s"Unexpected acknowledge response. Code [$status] Content-type [${response.entity.contentType}] Entity [$entity]"
            )
          }
      }
    }.withDefaultRetry

  private def pool[T: FromResponseUnmarshaller, Ctx](parallelism: Int)(
      implicit system: ActorSystem
  ): FlowWithContext[HttpRequest, Ctx, Try[T], Ctx, Future[HostConnectionPool]] =
    GoogleHttp().cachedHostConnectionPool[T, Ctx](
      PubSubGoogleApisHost,
      PubSubGoogleApisPort,
      https = !isEmulated,
      authenticate = !isEmulated,
      parallelism = parallelism
    )

  def publish[T](topic: String, parallelism: Int): FlowWithContext[PublishRequest, T, PublishResponse, T, NotUsed] =
    FlowWithContext.fromTuples {
      Flow
        .fromMaterializer { (mat, attr) =>
          import mat.executionContext
          implicit val system = mat.system
          implicit val settings = GoogleAttributes.resolveSettings(mat, attr)
          val url: Uri = s"/v1/projects/${settings.projectId}/topics/$topic:publish"
          FlowWithContext[PublishRequest, T]
            .mapAsync(parallelism) { request =>
              Marshal(request)
                .to[RequestEntity]
                .map { entity =>
                  HttpRequest(POST, url, entity = entity)
                }(ExecutionContexts.parasitic)
            }
            .via(pool[PublishResponse, T](parallelism))
            .map(_.get)
            .asFlow
        }
        .mapMaterializedValue(_ => NotUsed)
    }

  private implicit val publishResponseUnmarshaller: FromResponseUnmarshaller[PublishResponse] =
    Unmarshaller.withMaterializer { implicit ec => implicit mat => response: HttpResponse =>
      response.status match {
        case StatusCodes.Success(_) if response.entity.contentType == ContentTypes.`application/json` =>
          Unmarshal(response.entity).to[PublishResponse]
        case status =>
          Unmarshal(response.entity).to[String].map { entity =>
            throw new RuntimeException(
              s"Unexpected publish response. Code [$status] Content-type [${response.entity.contentType}] Entity [$entity]"
            )
          }
      }
    }.withDefaultRetry

}
