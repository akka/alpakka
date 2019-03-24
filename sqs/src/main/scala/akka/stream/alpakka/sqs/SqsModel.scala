/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import akka.annotation.InternalApi
import software.amazon.awssdk.core.SdkPojo
import software.amazon.awssdk.services.sqs.model.{Message, SqsResponseMetadata}

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.FiniteDuration

sealed abstract class MessageAction(val message: Message) {

  /** Java API */
  def getMessage: Message = message
}

object MessageAction {

  /**
   * Delete the message from the queue.
   *
   * @see [https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html DeleteMessage]
   */
  final class Delete private (message: Message) extends MessageAction(message) {
    override def toString: String = s"Delete($message)"

    override def equals(other: Any): Boolean = other match {
      case that: Delete => java.util.Objects.equals(this.message, that.message)
      case _ => false
    }

    override def hashCode(): Int = java.util.Objects.hash(message)
  }

  final object Delete {
    def apply(message: Message): MessageAction = new Delete(message)
  }

  /**
   * Ignore the message.
   */
  final class Ignore private (message: Message) extends MessageAction(message) {
    override def toString: String = s"Ignore($message)"

    override def equals(other: Any): Boolean = other match {
      case that: Ignore => java.util.Objects.equals(this.message, that.message)
      case _ => false
    }

    override def hashCode(): Int = java.util.Objects.hash(message)
  }

  final object Ignore {
    def apply(message: Message): MessageAction = new Ignore(message)
  }

  /**
   * Change the visibility timeout of the message.
   * The maximum allowed timeout value is 12 hours.
   *
   * @param visibilityTimeout new timeout in seconds
   * @see [https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ChangeMessageVisibility.html ChangeMessageVisibility]
   */
  final class ChangeMessageVisibility private (message: Message, val visibilityTimeout: Int)
      extends MessageAction(message) {
    // SQS requirements
    require(
      0 <= visibilityTimeout && visibilityTimeout <= 43200,
      s"Invalid value ($visibilityTimeout) for visibilityTimeout. Requirement: 0 <= visibilityTimeout <= 43200"
    )

    /** Java API */
    def getVisibilityTimeout: Int = visibilityTimeout

    override def toString: String = s"ChangeMessageVisibility($message, $visibilityTimeout)"

    override def equals(other: Any): Boolean = other match {
      case that: ChangeMessageVisibility =>
        java.util.Objects.equals(this.message, that.message) &&
        this.visibilityTimeout == that.visibilityTimeout
      case _ => false
    }

    override def hashCode(): Int = java.util.Objects.hash(message, visibilityTimeout: Integer)
  }

  object ChangeMessageVisibility {
    def apply(message: Message, visibilityTimeout: Int): MessageAction =
      new ChangeMessageVisibility(message, visibilityTimeout)

    def apply(message: Message, visibilityTimeout: FiniteDuration): MessageAction =
      new ChangeMessageVisibility(message, visibilityTimeout.toSeconds.toInt)
  }

  /**
   * Java API: Delete the message from the queue.
   */
  def delete(message: Message): MessageAction = Delete(message)

  /**
   * Java API: Ignore the message.
   */
  def ignore(message: Message): MessageAction = Ignore(message)

  /**
   * Java API: Change the visibility timeout of the message.
   *
   * @param message           the message to change
   * @param visibilityTimeout new timeout in seconds
   */
  def changeMessageVisibility(message: Message, visibilityTimeout: Int): MessageAction =
    ChangeMessageVisibility(message, visibilityTimeout)

  /**
   * Java API: Change the visibility timeout of the message.
   *
   * @param message           the message to change
   * @param visibilityTimeout new timeout
   */
  def changeMessageVisibility(message: Message, visibilityTimeout: java.time.Duration): MessageAction =
    ChangeMessageVisibility(message, visibilityTimeout.getSeconds.toInt)

}

/**
 * Messages returned by a SqsFlow.
 *
 * @param message the SQS message.
 */
final class SqsPublishResult[T <: SdkPojo] @InternalApi private[sqs] (
    val responseMetadata: SqsResponseMetadata,
    val metadata: T
) {

  /** Java API */
  def getResponseMetadata: SqsResponseMetadata = responseMetadata

  /** Java API */
  def getMetadata: T = metadata

  override def toString =
    s"""SqsPublishResult(responseMetadata=$responseMetadata, metadata=$metadata)"""

  override def equals(other: Any): Boolean = other match {
    case that: SqsPublishResult[T] =>
      java.util.Objects.equals(this.responseMetadata, that.responseMetadata) &&
      java.util.Objects.equals(this.metadata, that.metadata)
    case _ => false
  }

  override def hashCode(): Int = java.util.Objects.hash(responseMetadata, metadata)
}

final class SqsAckResult[T <: SdkPojo] @InternalApi private[sqs] (
    val responseMetadata: Option[SqsResponseMetadata],
    val metadata: Option[T],
    val messageAction: MessageAction
) {

  def this(responseMetadata: SqsResponseMetadata, metadata: T, messageAction: MessageAction) = {
    this(Some(responseMetadata), Some(metadata), messageAction)
  }

  def this(messageAction: MessageAction) = {
    this(None, None, messageAction)
  }

  /** Java API */
  def getResponseMetadata: java.util.Optional[SqsResponseMetadata] = responseMetadata.asJava

  /** Java API */
  def getMetadata: java.util.Optional[T] = metadata.asJava

  /** Java API */
  def getMessageAction: MessageAction = messageAction

  override def toString =
    s"""SqsAckResult(responseMetadata=$responseMetadata,metadata=$metadata,messageAction=$messageAction)"""

  override def equals(other: Any): Boolean = other match {
    case that: SqsAckResult[T] =>
      java.util.Objects.equals(this.responseMetadata, that.responseMetadata) &&
      java.util.Objects.equals(this.metadata, that.metadata) &&
      java.util.Objects.equals(this.messageAction, that.messageAction)
    case _ => false
  }

  override def hashCode(): Int =
    java.util.Objects.hash(metadata, messageAction)
}
