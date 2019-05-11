/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import akka.NotUsed
import akka.annotation.InternalApi
import software.amazon.awssdk.awscore.DefaultAwsResponseMetadata
import software.amazon.awssdk.services.sqs.model._

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
    override def toString: String = s"Delete(message=$message)"

    override def equals(other: Any): Boolean = other match {
      case that: Delete => java.util.Objects.equals(this.message, that.message)
      case _ => false
    }

    override def hashCode(): Int = java.util.Objects.hash(message)
  }

  final object Delete {
    def apply(message: Message): Delete = new Delete(message)
  }

  /**
   * Ignore the message.
   */
  final class Ignore private (message: Message) extends MessageAction(message) {
    override def toString: String = s"Ignore(message=$message)"

    override def equals(other: Any): Boolean = other match {
      case that: Ignore => java.util.Objects.equals(this.message, that.message)
      case _ => false
    }

    override def hashCode(): Int = java.util.Objects.hash(message)
  }

  final object Ignore {
    def apply(message: Message): Ignore = new Ignore(message)
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

    override def toString: String = s"ChangeMessageVisibility(message=$message,visibilityTimeout=$visibilityTimeout)"

    override def equals(other: Any): Boolean = other match {
      case that: ChangeMessageVisibility =>
        java.util.Objects.equals(this.message, that.message) &&
        this.visibilityTimeout == that.visibilityTimeout
      case _ => false
    }

    override def hashCode(): Int = java.util.Objects.hash(message, visibilityTimeout: Integer)
  }

  object ChangeMessageVisibility {
    def apply(message: Message, visibilityTimeout: Int): ChangeMessageVisibility =
      new ChangeMessageVisibility(message, visibilityTimeout)

    def apply(message: Message, visibilityTimeout: FiniteDuration): ChangeMessageVisibility =
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
 * Result contained in a Sqs Response.
 *
 */
sealed abstract class SqsResult {

  type Result

  /**
   * The SQS response metadata (AWS request ID, ...)
   */
  def responseMetadata: SqsResponseMetadata

  def result: Result

  /** Java API */
  def geResponseMetadata: SqsResponseMetadata = responseMetadata

  /** Java API */
  def getResult: Result = result
}

object SqsResult {
  private[sqs] val EmptyResponseMetadata: SqsResponseMetadata = SqsResponseMetadata.create(
    DefaultAwsResponseMetadata.create(java.util.Collections.emptyMap())
  )
}

/**
 * Messages returned by a SqsPublishFlow
 */
final class SqsPublishResult @InternalApi private[sqs] (
    val request: SendMessageRequest,
    response: SendMessageResponse
) extends SqsResult {

  override type Result = SendMessageResponse

  override def responseMetadata: SqsResponseMetadata = response.responseMetadata

  override def result: SendMessageResponse = response

  override def toString =
    s"""SqsPublishResult(request=$request,result=$result)"""

  override def equals(other: Any): Boolean = other match {
    case that: SqsPublishResult =>
      java.util.Objects.equals(this.request, that.request) &&
      java.util.Objects.equals(this.result, that.result)
    case _ => false
  }

  override def hashCode(): Int = java.util.Objects.hash(request, result)
}

/**
 * Messages returned by a SqsPublishFlow.grouped or batched
 */
final class SqsPublishResultEntry @InternalApi private[sqs] (
    val request: SendMessageRequest,
    override val result: SendMessageBatchResultEntry,
    override val responseMetadata: SqsResponseMetadata
) extends SqsResult {

  override type Result = SendMessageBatchResultEntry

  override def toString =
    s"""SqsPublishResultEntry(request=$request,result=$result)"""

  override def equals(other: Any): Boolean = other match {
    case that: SqsPublishResultEntry =>
      java.util.Objects.equals(this.request, that.request) &&
      java.util.Objects.equals(this.result, that.result)
    case _ => false
  }

  override def hashCode(): Int = java.util.Objects.hash(request, result)
}

/**
 * Messages returned by a SqsAckFlow
 *
 */
sealed abstract class SqsAckResult extends SqsResult {

  def messageAction: MessageAction

  /** Java API */
  def getMessageAction: MessageAction = messageAction

}

object SqsAckResult {

  /**
   * Delete acknowledgment
   * @param messageAction the delete message action
   * @param result the sqs DeleteMessageResponse
   */
  final class SqsDeleteResult @InternalApi private[sqs] (
      override val messageAction: MessageAction.Delete,
      override val result: DeleteMessageResponse
  ) extends SqsAckResult {

    override type Result = DeleteMessageResponse

    override def responseMetadata: SqsResponseMetadata = result.responseMetadata

    override def toString: String =
      s"SqsDeleteResult(messageAction=$messageAction,result=$result)"

    override def equals(other: Any): Boolean = other match {
      case that: SqsDeleteResult =>
        java.util.Objects.equals(this.messageAction, that.messageAction) &&
        java.util.Objects.equals(this.result, that.result)
      case _ => false
    }

    override def hashCode(): Int = java.util.Objects.hash(messageAction, result)
  }

  /**
   * Ignore acknowledgment
   * No requests are executed on the SQS service for ignore messageAction.
   * Its result is [[akka.NotUsed]] and the responseMetadata is always empty
   * @param messageAction the ignore message action
   */
  final class SqsIgnoreResult @InternalApi private[sqs] (
      override val messageAction: MessageAction.Ignore
  ) extends SqsAckResult {

    override type Result = NotUsed.type

    override def responseMetadata: SqsResponseMetadata = SqsResult.EmptyResponseMetadata

    override def result: Result = NotUsed

    override def toString: String =
      s"SqsIgnoreResult(messageAction=$messageAction)"

    override def equals(other: Any): Boolean = other match {
      case that: SqsIgnoreResult =>
        java.util.Objects.equals(this.messageAction, that.messageAction)
      case _ => false
    }

    override def hashCode(): Int = java.util.Objects.hash(messageAction)
  }

  /**
   * ChangeMessageVisibility acknowledgement
   * @param messageAction the change message visibility action
   * @param result the sqs ChangeMessageVisibilityResponse
   */
  final class SqsChangeMessageVisibilityResult @InternalApi private[sqs] (
      override val messageAction: MessageAction.ChangeMessageVisibility,
      override val result: ChangeMessageVisibilityResponse
  ) extends SqsAckResult {

    override type Result = ChangeMessageVisibilityResponse

    override def responseMetadata: SqsResponseMetadata = result.responseMetadata

    override def toString: String =
      s"SqsChangeMessageVisibilityResult(messageAction=$messageAction,result=$result)"

    override def equals(other: Any): Boolean = other match {
      case that: SqsChangeMessageVisibilityResult =>
        java.util.Objects.equals(this.messageAction, that.messageAction) &&
        java.util.Objects.equals(this.result, that.result)
      case _ => false
    }

    override def hashCode(): Int = java.util.Objects.hash(messageAction, result)
  }

}

/**
 * Messages returned by a SqsAckFlow.
 *
 */
sealed abstract class SqsAckResultEntry extends SqsResult {

  def messageAction: MessageAction

  /** Java API */
  def getMessageAction: MessageAction = messageAction

}

object SqsAckResultEntry {

  /**
   * Delete acknowledgement within a batch
   * @param messageAction the delete message action
   * @param result the sqs DeleteMessageBatchResultEntry
   */
  final class SqsDeleteResultEntry(override val messageAction: MessageAction.Delete,
                                   override val result: DeleteMessageBatchResultEntry,
                                   override val responseMetadata: SqsResponseMetadata)
      extends SqsAckResultEntry {

    override type Result = DeleteMessageBatchResultEntry

    override def toString: String =
      s"SqsDeleteResultEntry(messageAction=$messageAction,result=$result)"

    override def equals(other: Any): Boolean = other match {
      case that: SqsDeleteResultEntry =>
        java.util.Objects.equals(this.messageAction, that.messageAction) &&
        java.util.Objects.equals(this.result, that.result)
      case _ => false
    }

    override def hashCode(): Int = java.util.Objects.hash(messageAction, result)

  }

  /**
   * Ignore acknowledgment within a batch
   * No requests are executed on the SQS service for ignore messageAction.
   * Its result is [[akka.NotUsed]] and the responseMetadata is always empty
   * @param messageAction the ignore message action
   */
  final class SqsIgnoreResultEntry @InternalApi private[sqs] (
      override val messageAction: MessageAction.Ignore
  ) extends SqsAckResultEntry {

    override type Result = NotUsed.type

    override def responseMetadata: SqsResponseMetadata = SqsResult.EmptyResponseMetadata

    override def result: Result = NotUsed

    override def toString: String =
      s"SqsIgnoreResultEntry(messageAction=$messageAction)"

    override def equals(other: Any): Boolean = other match {
      case that: SqsIgnoreResultEntry =>
        java.util.Objects.equals(this.messageAction, that.messageAction)
      case _ => false
    }

    override def hashCode(): Int = java.util.Objects.hash(messageAction)
  }

  /**
   * ChangeMessageVisibility acknowledgement within a batch
   * @param messageAction the change message visibility action
   * @param result the sqs ChangeMessageVisibilityBatchResultEntry
   */
  final class SqsChangeMessageVisibilityResultEntry(override val messageAction: MessageAction.ChangeMessageVisibility,
                                                    override val result: ChangeMessageVisibilityBatchResultEntry,
                                                    override val responseMetadata: SqsResponseMetadata)
      extends SqsAckResultEntry {

    override type Result = ChangeMessageVisibilityBatchResultEntry

    override def toString: String =
      s"SqsChangeMessageVisibilityResultEntry(messageAction=$messageAction,result=$result)"

    override def equals(other: Any): Boolean = other match {
      case that: SqsChangeMessageVisibilityResultEntry =>
        java.util.Objects.equals(this.messageAction, that.messageAction) &&
        java.util.Objects.equals(this.result, that.result)
      case _ => false
    }

    override def hashCode(): Int = java.util.Objects.hash(messageAction, result)
  }

}
