/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import com.amazonaws.services.sqs.model.Message

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration

sealed abstract class MessageAction(val message: Message)

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
    def apply(message: Message, visibilityTimeout: Duration): MessageAction =
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
   * @param message the message to change
   * @param visibilityTimeout new timeout in seconds
   */
  def changeMessageVisibility(message: Message, visibilityTimeout: Int): MessageAction =
    ChangeMessageVisibility(message, visibilityTimeout)

  /**
   * Java API: Change the visibility timeout of the message.
   *
   * @param message the message to change
   * @param visibilityTimeout new timeout
   */
  def changeMessageVisibility(message: Message, visibilityTimeout: java.time.Duration): MessageAction =
    ChangeMessageVisibility(message, visibilityTimeout.getSeconds.toInt)

}

/**
 * Messages returned by a SqsFlow.
 * @param metadata metadata with AWS response details.
 * @param message message body.
 */
final class SqsPublishResult private (val metadata: com.amazonaws.services.sqs.model.SendMessageResult,
                                      val message: String) {

  /** Java API */
  def getMetadata: com.amazonaws.services.sqs.model.SendMessageResult = metadata

  /** Java API */
  def getMessage: String = message

  override def toString =
    s"""SqsPublishResult(metadata=$metadata,message=$message)"""

  override def equals(other: Any): Boolean = other match {
    case that: SqsPublishResult =>
      java.util.Objects.equals(this.metadata, that.metadata) &&
      java.util.Objects.equals(this.message, that.message)
    case _ => false
  }

  override def hashCode(): Int =
    java.util.Objects.hash(metadata, message)
}

object SqsPublishResult {

  /** Scala API */
  def apply(
      metadata: com.amazonaws.services.sqs.model.SendMessageResult,
      message: String
  ): SqsPublishResult = new SqsPublishResult(
    metadata,
    message
  )

  /** Java API */
  def create(
      metadata: com.amazonaws.services.sqs.model.SendMessageResult,
      message: String
  ): SqsPublishResult = new SqsPublishResult(
    metadata,
    message
  )
}

final class SqsAckResult private (
    val metadata: Option[com.amazonaws.AmazonWebServiceResult[com.amazonaws.ResponseMetadata]],
    val message: String
) {

  /** Java API */
  def getMetadata: java.util.Optional[com.amazonaws.AmazonWebServiceResult[com.amazonaws.ResponseMetadata]] =
    metadata.asJava

  /** Java API */
  def getMessage: String = message

  override def toString =
    s"""SqsAckResult(metadata=$metadata,message=$message)"""

  override def equals(other: Any): Boolean = other match {
    case that: SqsAckResult =>
      java.util.Objects.equals(this.metadata, that.metadata) &&
      java.util.Objects.equals(this.message, that.message)
    case _ => false
  }

  override def hashCode(): Int =
    java.util.Objects.hash(metadata, message)
}

object SqsAckResult {

  /** Scala API */
  def apply(
      metadata: Option[com.amazonaws.AmazonWebServiceResult[com.amazonaws.ResponseMetadata]],
      message: String
  ): SqsAckResult = new SqsAckResult(
    metadata,
    message
  )

  /** Java API */
  def create(
      metadata: java.util.Optional[com.amazonaws.AmazonWebServiceResult[com.amazonaws.ResponseMetadata]],
      message: String
  ): SqsAckResult = new SqsAckResult(
    metadata.asScala,
    message
  )
}
