/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.googlecloud.pubsub

import java.time.Instant
import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.stream.alpakka.google.GoogleSettings
import akka.stream.alpakka.google.auth.ServiceAccountCredentials

import scala.annotation.nowarn
import scala.collection.immutable
import scala.jdk.CollectionConverters._

/**
 * @param projectId (deprecated) the project Id in the google account
 * @param pullReturnImmediately when pulling messages, if there are non the API will wait or return immediately. Defaults to true.
 * @param pullMaxMessagesPerInternalBatch when pulling messages, the maximum that will be in the batch of messages. Defaults to 1000.
 */
class PubSubConfig private (
    /** @deprecated Use [[akka.stream.alpakka.google.GoogleSettings]] */ @deprecated(
      "Use akka.stream.alpakka.google.GoogleSettings",
      "3.0.0"
    ) @Deprecated val projectId: String,
    val pullReturnImmediately: Boolean,
    val pullMaxMessagesPerInternalBatch: Int,
    @deprecated("Added only to help with migration", "3.0.0") @InternalApi private[pubsub] val settings: Option[
      GoogleSettings
    ]
) {

  override def toString: String =
    s"PubSubConfig(projectId=$projectId)": @nowarn("msg=deprecated")
}

object PubSubConfig {

  def apply(): PubSubConfig = apply(true, 1000)

  def apply(pullReturnImmediately: Boolean, pullMaxMessagesPerInternalBatch: Int): PubSubConfig =
    new PubSubConfig("", pullReturnImmediately, pullMaxMessagesPerInternalBatch, None)

  def create(): PubSubConfig = apply()

  def create(pullReturnImmediately: Boolean, pullMaxMessagesPerInternalBatch: Int): PubSubConfig =
    apply(pullReturnImmediately, pullMaxMessagesPerInternalBatch)

  /**
   * @deprecated Use [[akka.stream.alpakka.google.GoogleSettings]] to manage credentials
   */
  @deprecated("Use akka.stream.alpakka.google.GoogleSettings to manage credentials", "3.0.0")
  @Deprecated
  def apply(projectId: String, clientEmail: String, privateKey: String)(
      implicit actorSystem: ActorSystem
  ): PubSubConfig =
    new PubSubConfig(
      projectId = projectId,
      pullReturnImmediately = true,
      pullMaxMessagesPerInternalBatch = 1000,
      Some(
        GoogleSettings
          .apply()
          .copy(
            projectId = projectId,
            credentials = ServiceAccountCredentials(projectId,
                                                    clientEmail,
                                                    privateKey,
                                                    Seq("https://www.googleapis.com/auth/pubsub"))
          )
      )
    )

  /**
   * @deprecated Use [[akka.stream.alpakka.google.GoogleSettings]] to manage credentials
   */
  @deprecated("Use akka.stream.alpakka.google.GoogleSettings to manage credentials", "3.0.0")
  @Deprecated
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
      Some(
        GoogleSettings
          .apply()
          .copy(
            projectId = projectId,
            credentials = ServiceAccountCredentials(projectId,
                                                    clientEmail,
                                                    privateKey,
                                                    Seq("https://www.googleapis.com/auth/pubsub"))
          )
      )
    )

  /**
   * Java API
   * @deprecated Use [[akka.stream.alpakka.google.GoogleSettings]] to manage credentials
   */
  @deprecated("Use akka.stream.alpakka.google.GoogleSettings to manage credentials", "3.0.0")
  @Deprecated
  def create(projectId: String, clientEmail: String, privateKey: String, actorSystem: ActorSystem): PubSubConfig =
    apply(projectId, clientEmail, privateKey)(actorSystem)

  /**
   * Java API
   * @deprecated Use [[akka.stream.alpakka.google.GoogleSettings]] to manage credentials
   */
  @deprecated("Use akka.stream.alpakka.google.GoogleSettings to manage credentials", "3.0.0")
  @Deprecated
  def create(projectId: String,
             clientEmail: String,
             privateKey: String,
             actorSystem: ActorSystem,
             pullReturnImmediately: Boolean,
             pullMaxMessagesPerInternalBatch: Int): PubSubConfig =
    apply(projectId, clientEmail, privateKey, pullReturnImmediately, pullMaxMessagesPerInternalBatch)(actorSystem)
}

final class PublishMessage private (val data: String,
                                    val attributes: Option[immutable.Map[String, String]],
                                    val orderingKey: Option[String]) {
  def this(data: String, attributes: Option[immutable.Map[String, String]]) = this(data, attributes, None)

  override def toString: String =
    "PublishMessage(data=" + data + ",attributes=" + attributes.toString + ",orderingKey=" + orderingKey + ")"

  override def equals(other: Any): Boolean = other match {
    case that: PublishMessage => data == that.data && attributes == that.attributes && orderingKey == that.orderingKey
    case _ => false
  }

  override def hashCode: Int = java.util.Objects.hash(data, attributes, orderingKey)
}

object PublishMessage {
  def apply(data: String, attributes: immutable.Map[String, String]) = new PublishMessage(data, Some(attributes), None)
  def apply(data: String, attributes: Option[immutable.Map[String, String]], orderingKey: Option[String]) =
    new PublishMessage(data, attributes, orderingKey)
  def apply(data: String, attributes: Option[immutable.Map[String, String]]) =
    new PublishMessage(data, attributes, None)
  def apply(data: String) = new PublishMessage(data, None, None)
  def create(data: String) = new PublishMessage(data, None, None)

  /**
   * Java API
   */
  def create(data: String, attributes: java.util.Map[String, String]): PublishMessage =
    create(data, attributes, java.util.Optional.empty())

  /**
   * Java API with ordering key
   */
  def create(data: String,
             attributes: java.util.Map[String, String],
             orderingKey: java.util.Optional[String]): PublishMessage =
    new PublishMessage(data, Some(attributes.asScala.toMap), Option(orderingKey.orElse(null)))
}

/**
 * 'data' of [[ReceivedMessage]].
 * @param data the base64 encoded data, if not present, attributes have to contain at least one entry
 * @param attributes attributes for this message, if not present, data can't be empty
 * @param messageId the message id given by server.
 * @param publishTime the time the message was published.
 * @param orderingKey if non-empty, identifies related messages for which publish order should be respected
 */
final class PubSubMessage private (val data: Option[String],
                                   val attributes: Option[immutable.Map[String, String]],
                                   val messageId: String,
                                   val publishTime: Instant,
                                   val orderingKey: Option[String]) {

  def this(data: Option[String],
           attributes: Option[immutable.Map[String, String]],
           messageId: String,
           publishTime: Instant) = this(data, attributes, messageId, publishTime, None)

  def withAttributes(attributes: java.util.Map[String, String]): PubSubMessage =
    new PubSubMessage(data, Some(attributes.asScala.toMap), messageId, publishTime, orderingKey)

  def withData(data: String): PubSubMessage =
    new PubSubMessage(Some(data), attributes, messageId, publishTime, orderingKey)

  def withOrderingKey(orderingKey: String): PubSubMessage =
    new PubSubMessage(data, attributes, messageId, publishTime, Some(orderingKey))

  override def equals(other: Any): Boolean = other match {
    case that: PubSubMessage =>
      data == that.data && attributes == that.attributes && messageId == that.messageId && publishTime == that.publishTime && orderingKey == that.orderingKey
    case _ => false
  }

  override def hashCode: Int = java.util.Objects.hash(data, attributes, messageId, publishTime, orderingKey)

  override def toString: String =
    "PubSubMessage(data=" + data + ",attributes=" + attributes + ",messageId=" + messageId + ",publishTime=" + publishTime + ",orderingKey=" + orderingKey + ")"
}

object PubSubMessage {

  def apply(data: Option[String],
            attributes: Option[immutable.Map[String, String]],
            messageId: String,
            publishTime: Instant) =
    new PubSubMessage(data, attributes, messageId, publishTime, None)

  def apply(data: Option[String] = None,
            attributes: Option[immutable.Map[String, String]] = None,
            messageId: String,
            publishTime: Instant,
            orderingKey: Option[String] = None) =
    new PubSubMessage(data, attributes, messageId, publishTime, orderingKey)

  /**
   * Java API
   */
  def create(data: java.util.Optional[String],
             attributes: java.util.Optional[java.util.Map[String, String]],
             messageId: String,
             publishTime: Instant): PubSubMessage =
    create(data, attributes, messageId, publishTime, java.util.Optional.empty())

  /**
   * Java API with ordering key
   */
  def create(data: java.util.Optional[String],
             attributes: java.util.Optional[java.util.Map[String, String]],
             messageId: String,
             publishTime: Instant,
             orderingKey: java.util.Optional[String]): PubSubMessage =
    new PubSubMessage(Option(data.orElse(null)),
                      Option(attributes.orElse(null)).map(_.asScala.toMap),
                      messageId,
                      publishTime,
                      Option(orderingKey.orElse(null)))

}

final class PublishRequest private (val messages: immutable.Seq[PublishMessage]) {

  override def equals(other: Any): Boolean = other match {
    case that: PublishRequest => messages == that.messages
    case _ => false
  }

  override def hashCode: Int = messages.hashCode

  override def toString: String = "PublishRequest(" + messages.mkString("[", ",", "]") + ")"
}

object PublishRequest {

  def apply(messages: immutable.Seq[PublishMessage]): PublishRequest = new PublishRequest(messages)

  /**
   * Java API
   */
  def create(messages: java.util.List[PublishMessage]): PublishRequest =
    new PublishRequest(messages.asScala.toList)
}

/**
 * A message as it is received
 * @param ackId acknowledgement id. This id is used to tell pub/sub the message has been processed.
 * @param message the pubsub message including its data.
 */
final class ReceivedMessage private (val ackId: String, val message: PubSubMessage) {

  override def equals(other: Any): Boolean = other match {
    case that: ReceivedMessage => ackId == that.ackId && message == that.message
    case _ => false
  }

  override def hashCode: Int = java.util.Objects.hash(ackId, message)

  override def toString: String = "ReceivedMessage(ackId=" + ackId.toString + ",message=" + message.toString + ")"
}

object ReceivedMessage {

  def apply(ackId: String, message: PubSubMessage): ReceivedMessage =
    new ReceivedMessage(ackId, message)

  def create(ackId: String, message: PubSubMessage): ReceivedMessage =
    new ReceivedMessage(ackId, message)
}

final class AcknowledgeRequest private (val ackIds: immutable.Seq[String]) {

  override def equals(other: Any): Boolean = other match {
    case that: AcknowledgeRequest => ackIds == that.ackIds
    case _ => false
  }

  override def hashCode: Int = ackIds.hashCode

  override def toString: String = "AcknowledgeRequest(" + ackIds.mkString("[", ",", "]") + ")"
}

object AcknowledgeRequest {

  def apply(ackIds: String*): AcknowledgeRequest =
    new AcknowledgeRequest(ackIds.toList)

  /**
   * Java API
   */
  def create(ackIds: java.util.List[String]): AcknowledgeRequest =
    new AcknowledgeRequest(ackIds.asScala.toList)
}

private final class PublishResponse private (val messageIds: immutable.Seq[String]) {

  override def equals(other: Any): Boolean = other match {
    case that: PublishResponse => messageIds == that.messageIds
    case _ => false
  }

  override def hashCode: Int = messageIds.hashCode

  override def toString: String = "PublishResponse(" + messageIds.mkString("[", ",", "]") + ")"
}

object PublishResponse {

  @InternalApi private[pubsub] def apply(messageIds: immutable.Seq[String]): PublishResponse =
    new PublishResponse(messageIds)
}

@InternalApi
private[pubsub] final case class PullRequest(returnImmediately: Boolean, maxMessages: Int)

@InternalApi
private final class PullResponse private[pubsub] (val receivedMessages: Option[immutable.Seq[ReceivedMessage]]) {

  override def equals(other: Any): Boolean = other match {
    case that: PullResponse => receivedMessages == that.receivedMessages
    case _ => false
  }

  override def hashCode: Int = receivedMessages.hashCode

  override def toString: String = "PullResponse(" + receivedMessages.map(_.mkString("[", ",", "]")) + ")"
}

object PullResponse {

  @InternalApi private[pubsub] def apply(receivedMessages: Option[immutable.Seq[ReceivedMessage]]) =
    new PullResponse(receivedMessages)

}
