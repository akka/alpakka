/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub

import java.time.Instant

import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.http.scaladsl.{Http, HttpExt}
import akka.stream.alpakka.googlecloud.pubsub.impl.{GoogleSession, GoogleTokenApi}

import scala.collection.immutable
import scala.collection.JavaConverters._

private[pubsub] class PubSubConfig(val projectId: String,
                                   val apiKey: String,
                                   clientEmail: String,
                                   privateKey: String,
                                   http: => HttpExt) {
  @InternalApi
  private[pubsub] lazy val session = new GoogleSession(clientEmail, privateKey, new GoogleTokenApi(http))
}

object PubSubConfig {
  def apply(projectId: String, apiKey: String, clientEmail: String, privateKey: String)(
      implicit actorSystem: ActorSystem
  ) = new PubSubConfig(projectId, apiKey, clientEmail, privateKey, Http())

  def create(projectId: String, apiKey: String, clientEmail: String, privateKey: String, actorSystem: ActorSystem): PubSubConfig = {
    apply(projectId, apiKey, clientEmail, privateKey)(actorSystem)
  }
}

final case class PubSubMessage(data: String,
                               messageId: String,
                               attributes: Option[immutable.Map[String, String]] = None,
                               publishTime: Option[Instant] = None) {

  def withAttributes(attributes: java.util.Map[String, String]): PubSubMessage =
    copy(attributes = Some(attributes.asScala.toMap))

  def withPublishTime(publishTime: Instant): PubSubMessage =
    copy(publishTime = Some(publishTime))

}

object PubSubMessage {

  /**
   * Java API: create [[PubSubMessage]]
   */
  def create(data: String, messageId: String) =
    PubSubMessage(data, messageId)
}

final case class PublishRequest(messages: immutable.Seq[PubSubMessage])

object PublishRequest {
  def of(messages: java.util.List[PubSubMessage]): PublishRequest =
    PublishRequest(messages.asScala.toList)
}

final case class ReceivedMessage(ackId: String, message: PubSubMessage)

final case class AcknowledgeRequest(ackIds: immutable.Seq[String])

object AcknowledgeRequest {
  def of(ackIds: java.util.List[String]): AcknowledgeRequest =
    AcknowledgeRequest(ackIds.asScala.toList)
}

private final case class PublishResponse(messageIds: immutable.Seq[String])

private final case class PullResponse(receivedMessages: Option[immutable.Seq[ReceivedMessage]])
