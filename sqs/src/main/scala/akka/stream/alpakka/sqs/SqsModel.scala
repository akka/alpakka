/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import akka.annotation.{ApiMayChange, InternalApi}
import software.amazon.awssdk.services.sqs.model.{Message, SqsResponse}

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
 * Additional identifiers for FIFO messsages
 *
 * @see [https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-additional-fifo-queue-identifiers.html]
 */
final class FifoMessageIdentifiers @InternalApi private[sqs] (
    val sequenceNumber: String,
    val messageGroupId: String,
    val messageDeduplicationId: Option[String]
) {

  /** Java API */
  def getSequenceNumber: String = sequenceNumber

  /** Java API */
  def getMessageGroupId: String = messageGroupId

  /** Java API */
  def getMessageDeduplicationId: java.util.Optional[String] = messageDeduplicationId.asJava

  override def toString: String =
    s"FifoMessageIdentifiers(sequenceNumber=$sequenceNumber, messageGroupId=$messageGroupId, messageDeduplicationId=$messageDeduplicationId)"

  override def equals(other: Any): Boolean = other match {
    case that: FifoMessageIdentifiers =>
      java.util.Objects.equals(this.sequenceNumber, that.sequenceNumber) &&
      java.util.Objects.equals(this.messageGroupId, that.messageGroupId) &&
      java.util.Objects.equals(this.messageDeduplicationId, that.messageDeduplicationId)
    case _ => false
  }

  override def hashCode(): Int = java.util.Objects.hash(sequenceNumber, messageGroupId, messageDeduplicationId)
}

/**
 * Messages returned by a SqsFlow.
 *
 * @param message the SQS message.
 */
@ApiMayChange
final class SqsPublishResult[T <: SqsResponse] @InternalApi private[sqs] (
    val metadata: T,
    val fifoMessageIdentifiers: Option[FifoMessageIdentifiers]
) {

  /** Java API */
  def getMetadata: T = metadata

  /** Java API */
  def getFifoMessageIdentifiers: java.util.Optional[FifoMessageIdentifiers] = fifoMessageIdentifiers.asJava

  override def toString =
    s"""SqsPublishResult(metadata=$metadata, fifoMessageIdentifiers=$fifoMessageIdentifiers)"""

  override def equals(other: Any): Boolean = other match {
    case that: SqsPublishResult[T] =>
      java.util.Objects.equals(this.metadata, that.metadata) &&
      java.util.Objects.equals(this.fifoMessageIdentifiers, that.fifoMessageIdentifiers)
    case _ => false
  }

  override def hashCode(): Int = java.util.Objects.hash(metadata, fifoMessageIdentifiers)
}

@ApiMayChange
final class SqsAckResult[T <: SqsResponse] @InternalApi private[sqs] (val metadata: Option[T],
                                                                      val messageAction: MessageAction) {

  /** Java API */
  def getMetadata: java.util.Optional[T] = metadata.asJava

  /** Java API */
  def getMessageAction: MessageAction = messageAction

  override def toString =
    s"""SqsAckResult(metadata=$metadata,messageAction=$messageAction)"""

  override def equals(other: Any): Boolean = other match {
    case that: SqsAckResult[T] =>
      java.util.Objects.equals(this.metadata, that.metadata) &&
      java.util.Objects.equals(this.messageAction, that.messageAction)
    case _ => false
  }

  override def hashCode(): Int =
    java.util.Objects.hash(metadata, messageAction)
}
