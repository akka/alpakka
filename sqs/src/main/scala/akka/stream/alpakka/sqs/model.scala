/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import com.amazonaws.services.sqs.model.Message

import scala.compat.java8.OptionConverters._

sealed abstract class MessageAction

object MessageAction {

  /**
   * Delete the message from the queue.
   *
   * @see [https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html DeleteMessage]
   */
  final case object Delete extends MessageAction {
    def apply(message: Message): MessageActionPair = MessageActionPair(message, Delete)
  }

  /**
   * Ignore the message.
   */
  final case object Ignore extends MessageAction {
    def apply(message: Message): MessageActionPair = MessageActionPair(message, Ignore)
  }

  /**
   * Change the visibility timeout of the message.
   * The maximum allowed timeout value is 12 hours.
   *
   * @param visibilityTimeout new timeout in seconds
   * @see [https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ChangeMessageVisibility.html ChangeMessageVisibility]
   */
  final case class ChangeMessageVisibility(visibilityTimeout: Int) extends MessageAction {
    // SQS requirements
    require(
      0 <= visibilityTimeout && visibilityTimeout <= 43200,
      s"Invalid value ($visibilityTimeout) for visibilityTimeout. Requirement: 0 <= waitTimeSeconds <= 43200"
    )
  }

  object ChangeMessageVisibility {
    def apply(message: Message, visibilityTimeout: Int): MessageActionPair =
      MessageActionPair(message, ChangeMessageVisibility(visibilityTimeout))
  }

  /**
   * Java API: Delete the message from the queue.
   */
  def delete: MessageAction = Delete

  /**
   * Java API: Delete the message from the queue.
   */
  def delete(message: Message): MessageActionPair = Delete(message)

  /**
   * Java API: Ignore the message.
   */
  def ignore: MessageAction = Ignore

  /**
   * Java API: Ignore the message.
   */
  def ignore(message: Message): MessageActionPair = Ignore(message)

  /**
   * Java API: Change the visibility timeout of the message.
   *
   * @param visibilityTimeout new timeout in seconds
   */
  def changeMessageVisibility(visibilityTimeout: Int): MessageAction = ChangeMessageVisibility(visibilityTimeout)

  /**
   * Java API: Change the visibility timeout of the message.
   *
   * @param message the message to change
   * @param visibilityTimeout new timeout in seconds
   */
  def changeMessageVisibility(message: Message, visibilityTimeout: Int): MessageActionPair =
    ChangeMessageVisibility(message, visibilityTimeout)

}

final class MessageActionPair private (val message: com.amazonaws.services.sqs.model.Message,
                                       val action: MessageAction) {

  def withMessage(value: com.amazonaws.services.sqs.model.Message): MessageActionPair = copy(message = value)
  def withAction(value: MessageAction): MessageActionPair = copy(action = value)

  private def copy(message: com.amazonaws.services.sqs.model.Message = message,
                   action: MessageAction = action): MessageActionPair =
    new MessageActionPair(message = message, action = action)

  override def toString =
    s"""package$MessageActionPair(message=$message,action=$action)"""

  override def equals(other: Any): Boolean = other match {
    case that: MessageActionPair =>
      java.util.Objects.equals(this.message, that.message) &&
      java.util.Objects.equals(this.action, that.action)
    case _ => false
  }

  override def hashCode(): Int =
    java.util.Objects.hash(message, action)
}

object MessageActionPair {

  /** Scala API */
  def apply(
      message: com.amazonaws.services.sqs.model.Message,
      action: MessageAction
  ): MessageActionPair = new MessageActionPair(
    message,
    action
  )

  /** Java API */
  def create(
      message: com.amazonaws.services.sqs.model.Message,
      action: MessageAction
  ): MessageActionPair = new MessageActionPair(
    message,
    action
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

  def withMetadata(value: com.amazonaws.AmazonWebServiceResult[com.amazonaws.ResponseMetadata]): AckResult =
    copy(metadata = Option(value))
  def withMessage(value: String): AckResult = copy(message = value)

  private def copy(metadata: Option[com.amazonaws.AmazonWebServiceResult[com.amazonaws.ResponseMetadata]] = metadata,
                   message: String = message): AckResult = new AckResult(metadata = metadata, message = message)

  override def toString =
    s"""AckResult(metadata=$metadata,message=$message)"""

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
