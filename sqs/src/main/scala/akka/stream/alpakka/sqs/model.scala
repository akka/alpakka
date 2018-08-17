/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import com.amazonaws.services.sqs.model.Message

import scala.compat.java8.OptionConverters._

sealed abstract class MessageAction(val message: Message)

object MessageAction {

  /**
   * Delete the message from the queue.
   *
   * @see [https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html DeleteMessage]
   */
  final class Delete private (message: Message) extends MessageAction(message) {
    override def toString: String = s"Delete($message)"
  }

  final object Delete {
    def apply(message: Message): MessageAction = new Delete(message)
  }

  /**
   * Ignore the message.
   */
  final class Ignore private (message: Message) extends MessageAction(message) {
    override def toString: String = s"Ignore($message)"
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
      s"Invalid value ($visibilityTimeout) for visibilityTimeout. Requirement: 0 <= waitTimeSeconds <= 43200"
    )
    override def toString: String = s"Delete($message)"
  }

  object ChangeMessageVisibility {
    def apply(message: Message, visibilityTimeout: Int): MessageAction =
      new ChangeMessageVisibility(message, visibilityTimeout)
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

}

/**
 * Messages returned by a SqsFlow.
 * @param metadata metadata with AWS response details.
 * @param message message body.
 */
final class Result private (val metadata: com.amazonaws.services.sqs.model.SendMessageResult, val message: String) {

  /** Java API */
  def getMetadata: com.amazonaws.services.sqs.model.SendMessageResult = metadata

  /** Java API */
  def getMessage: String = message

  override def toString =
    s"""Result(metadata=$metadata,message=$message)"""

  override def equals(other: Any): Boolean = other match {
    case that: Result =>
      java.util.Objects.equals(this.metadata, that.metadata) &&
      java.util.Objects.equals(this.message, that.message)
    case _ => false
  }

  override def hashCode(): Int =
    java.util.Objects.hash(metadata, message)
}

object Result {

  /** Scala API */
  def apply(
      metadata: com.amazonaws.services.sqs.model.SendMessageResult,
      message: String
  ): Result = new Result(
    metadata,
    message
  )

  /** Java API */
  def create(
      metadata: com.amazonaws.services.sqs.model.SendMessageResult,
      message: String
  ): Result = new Result(
    metadata,
    message
  )
}

final class AckResult private (
    val metadata: Option[com.amazonaws.AmazonWebServiceResult[com.amazonaws.ResponseMetadata]],
    val message: String
) {

  /** Java API */
  def getMetadata: java.util.Optional[com.amazonaws.AmazonWebServiceResult[com.amazonaws.ResponseMetadata]] =
    metadata.asJava

  /** Java API */
  def getMessage: String = message

  override def toString =
    s"""AckResult(metadata=$metadata,message=$message)"""

  override def equals(other: Any): Boolean = other match {
    case that: AckResult =>
      java.util.Objects.equals(this.metadata, that.metadata) &&
      java.util.Objects.equals(this.message, that.message)
    case _ => false
  }

  override def hashCode(): Int =
    java.util.Objects.hash(metadata, message)
}

object AckResult {

  /** Scala API */
  def apply(
      metadata: Option[com.amazonaws.AmazonWebServiceResult[com.amazonaws.ResponseMetadata]],
      message: String
  ): AckResult = new AckResult(
    metadata,
    message
  )

  /** Java API */
  def create(
      metadata: java.util.Optional[com.amazonaws.AmazonWebServiceResult[com.amazonaws.ResponseMetadata]],
      message: String
  ): AckResult = new AckResult(
    metadata.asScala,
    message
  )
}
