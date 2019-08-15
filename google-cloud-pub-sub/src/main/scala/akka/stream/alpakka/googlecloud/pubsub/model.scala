/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub

import java.time.Instant

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.http.scaladsl.Http
import akka.stream.alpakka.googlecloud.pubsub.impl.{GoogleSession, GoogleTokenApi}

import scala.collection.immutable
import scala.collection.JavaConverters._

/**
 * @param projectId the project Id in the google account
 * @param pullReturnImmediately when pulling messages, if there are non the API will wait or return immediately. Defaults to true.
 * @param pullMaxMessagesPerInternalBatch when pulling messages, the maximum that will be in the batch of messages. Defaults to 1000.
 */
class PubSubConfig private (val projectId: String,
                            val pullReturnImmediately: Boolean,
                            val pullMaxMessagesPerInternalBatch: Int,
                            /**
                             * Internal API
                             */
                            @InternalApi private[pubsub] val session: GoogleSession) {

  /**
   * Internal API
   */
  @InternalApi private[pubsub] def withSession(session: GoogleSession) =
    copy(session = session)

  private def copy(session: GoogleSession) =
    new PubSubConfig(projectId, pullReturnImmediately, pullMaxMessagesPerInternalBatch, session)

  override def toString: String =
    s"PubSubConfig(projectId=$projectId)"
}

object PubSubConfig {
  def apply(projectId: String, clientEmail: String, privateKey: String)(
      implicit actorSystem: ActorSystem
  ): PubSubConfig =
    new PubSubConfig(
      projectId = projectId,
      pullReturnImmediately = true,
      pullMaxMessagesPerInternalBatch = 1000,
      session = new GoogleSession(clientEmail, privateKey, new GoogleTokenApi(Http()))
    )

  def apply(projectId: String,
            clientEmail: String,
            privateKey: String,
            pullReturnImmediately: Boolean,
            pullMaxMessagesPerInternalBatch: Int)(
      implicit actorSystem: ActorSystem
  ): PubSubConfig =
    new PubSubConfig(
      projectId = projectId,
      pullReturnImmediately = pullReturnImmediately,
      pullMaxMessagesPerInternalBatch = pullMaxMessagesPerInternalBatch,
      session = new GoogleSession(clientEmail, privateKey, new GoogleTokenApi(Http()))
    )

  def create(projectId: String, clientEmail: String, privateKey: String, actorSystem: ActorSystem): PubSubConfig =
    apply(projectId, clientEmail, privateKey)(actorSystem)

  def create(projectId: String,
             clientEmail: String,
             privateKey: String,
             actorSystem: ActorSystem,
             pullReturnImmediately: Boolean,
             pullMaxMessagesPerInternalBatch: Int): PubSubConfig =
    apply(projectId, clientEmail, privateKey, pullReturnImmediately, pullMaxMessagesPerInternalBatch)(actorSystem)
}

/**
 *
 * @param data the base64 encoded data
 * @param messageId the message id given by server. It must not be populated when publishing.
 * @param attributes optional extra attributes for this message.
 * @param publishTime the time the message was published. It must not be populated when publishing.
 */
final case class PubSubMessage(data: String,
                               //Should be Option[String]. '""' is used as default when creating messages for publishing.
                               messageId: String,
                               attributes: Option[immutable.Map[String, String]] = None,
                               publishTime: Option[Instant] = None) {

  def withAttributes(attributes: java.util.Map[String, String]): PubSubMessage =
    copy(attributes = Some(attributes.asScala.toMap))

  def withPublishTime(publishTime: Instant): PubSubMessage =
    copy(publishTime = Some(publishTime))

}

object PubSubMessage {

  def apply(data: String): PubSubMessage = PubSubMessage(data, "")

  def apply(data: String, attributes: immutable.Map[String, String]): PubSubMessage =
    PubSubMessage(data, "", Some(attributes), None)

  /**
   * Java API: create [[PubSubMessage]]
   */
  def create(data: String) =
    PubSubMessage(data)

  @deprecated("Setting messageId when creating message for publishing is futile.", "1.1.0")
  def create(data: String, messageId: String) =
    PubSubMessage(data, messageId)
}

final case class PublishRequest(messages: immutable.Seq[PubSubMessage])

object PublishRequest {
  def of(messages: java.util.List[PubSubMessage]): PublishRequest =
    PublishRequest(messages.asScala.toList)
}

/**
 * A message as it is received
 * @param ackId acknowledgement id. This id is used to tell pub/sub the message has been processed.
 * @param message the pubsub message including its data.
 */
final case class ReceivedMessage(ackId: String, message: PubSubMessage)

final case class AcknowledgeRequest(ackIds: immutable.Seq[String])

object AcknowledgeRequest {
  def of(ackIds: java.util.List[String]): AcknowledgeRequest =
    AcknowledgeRequest(ackIds.asScala.toList)
}

private final case class PublishResponse(messageIds: immutable.Seq[String])

private final case class PullResponse(receivedMessages: Option[immutable.Seq[ReceivedMessage]])
