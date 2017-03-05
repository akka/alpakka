/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.googlepubsub

case class PubSubMessage(data: String, messageId: String)

case class PublishRequest(messages: Seq[PubSubMessage])

case class ReceivedMessage(ackId: String, message: PubSubMessage)

case class AcknowledgeRequest(ackIds: Seq[String])

private case class PublishResponse(messageIds: Seq[String])

private case class PullResponse(receivedMessages: Option[Seq[ReceivedMessage]])

private case class OAuthResponse(access_token: String, token_type: String, expires_in: Int)

private case class AccessTokenExpiry(accessToken: String, expiresAt: Long)
