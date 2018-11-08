/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import com.amazonaws.services.sqs.model.Message

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration

sealed trait MessageAction {
  def message: Message
}

object MessageAction {

  /**
   * Delete the message from the queue.
   *
   * @see [https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html DeleteMessage]
   */
  final case class Delete(message: Message) extends MessageAction

  /**
   * Ignore the message.
   */
  final case class Ignore(message: Message) extends MessageAction

  /**
   * Change the visibility timeout of the message.
   * The maximum allowed timeout value is 12 hours.
   *
   * @param visibilityTimeout new timeout in seconds
   * @see [https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ChangeMessageVisibility.html ChangeMessageVisibility]
   */
  final case class ChangeMessageVisibility(message: Message, visibilityTimeout: Int) extends MessageAction {
    // SQS requirements
    require(
      0 <= visibilityTimeout && visibilityTimeout <= 43200,
      s"Invalid value ($visibilityTimeout) for visibilityTimeout. Requirement: 0 <= visibilityTimeout <= 43200"
    )
  }

  object ChangeMessageVisibility {
    def apply(message: Message, visibilityTimeout: Duration): MessageAction =
      ChangeMessageVisibility(message, visibilityTimeout.toSeconds.toInt)
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
 * @param metadata metadata with AWS response details.
 * @param message  message body.
 */
final case class SqsPublishResult(metadata: com.amazonaws.services.sqs.model.SendMessageResult, message: String) {

  /** Java API */
  def getMetadata: com.amazonaws.services.sqs.model.SendMessageResult = metadata

  /** Java API */
  def getMessage: String = message
}

object SqsPublishResult {

  /** Java API */
  def create(
      metadata: com.amazonaws.services.sqs.model.SendMessageResult,
      message: String
  ): SqsPublishResult = SqsPublishResult(
    metadata,
    message
  )
}

final case class SqsAckResult(
    metadata: Option[com.amazonaws.AmazonWebServiceResult[com.amazonaws.ResponseMetadata]],
    message: String
) {

  /** Java API */
  def getMetadata: java.util.Optional[com.amazonaws.AmazonWebServiceResult[com.amazonaws.ResponseMetadata]] =
    metadata.asJava

  /** Java API */
  def getMessage: String = message
}

object SqsAckResult {

  /** Java API */
  def create(
      metadata: java.util.Optional[com.amazonaws.AmazonWebServiceResult[com.amazonaws.ResponseMetadata]],
      message: String
  ): SqsAckResult = SqsAckResult(
    metadata.asScala,
    message
  )
}
