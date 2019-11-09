/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import akka.NotUsed
import akka.annotation.InternalApi
import software.amazon.awssdk.awscore.DefaultAwsResponseMetadata
import software.amazon.awssdk.services.sqs.model._

import scala.collection.Iterator
import scala.collection.JavaConverters._
import scala.collection.immutable.Iterable
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

  type Request <: AnyRef

  type Result <: AnyRef

  /**
   * The SQS response metadata (AWS request ID, ...)
   */
  def responseMetadata: SqsResponseMetadata

  def request: Request

  def result: Result

  /** Java API */
  def geResponseMetadata: SqsResponseMetadata = responseMetadata

  /** Java API */
  def getResult: Result = result
}

object SqsResult {
  private[sqs] val EmptySqsResponseMetadata: SqsResponseMetadata = SqsResponseMetadata.create(
    DefaultAwsResponseMetadata.create(java.util.Collections.emptyMap())
  )
}

sealed abstract class SqsResultEntry extends SqsResult

final class SqsBatchResult[T <: SqsResultEntry] @InternalApi private[sqs] (
    val successful: List[T],
    val failed: List[SqsResultErrorEntry[T#Request]]
) {

  @InternalApi def entries: Iterable[T] =
    if (failed.isEmpty) {
      successful
    } else {
      val iterator = new Iterator[T] {

        var entries: List[T] = successful

        override def hasNext: Boolean = entries.nonEmpty || failed.nonEmpty

        override def next(): T = entries match {
          case e :: es =>
            entries = es
            e
          case Nil if failed.nonEmpty => throw new SqsBatchException(failed)
          case Nil => Iterator.empty.next()
        }
      }
      iterator.toStream
    }

  @InternalApi def getEntries: java.lang.Iterable[T] = entries.asJava

  /** Java API */
  def getSuccessful: java.util.List[T] = successful.asJava

  /** Java API */
  def getFailed: java.util.List[SqsResultErrorEntry[T#Request]] = failed.asJava
}

/**
 * Messages returned by a SqsPublishFlow
 */
final class SqsPublishResult @InternalApi private[sqs] (
    override val request: SendMessageRequest,
    response: SendMessageResponse
) extends SqsResult {

  override type Request = SendMessageRequest

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
    override val request: SendMessageRequest,
    override val result: SendMessageBatchResultEntry,
    override val responseMetadata: SqsResponseMetadata
) extends SqsResultEntry {

  override type Request = SendMessageRequest

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

final class SqsResultErrorEntry[T <: AnyRef] @InternalApi private[sqs] (
    override val request: T,
    override val result: BatchResultErrorEntry,
    override val responseMetadata: SqsResponseMetadata
) extends SqsResultEntry {

  override type Request = T

  override type Result = BatchResultErrorEntry

  /** Java API */
  def getRequest: T = request

  override def toString: String = s"SqsResultErrorEntry(request=$request,result=$result)"

  override def equals(other: Any): Boolean = other match {
    case that: SqsResultErrorEntry[T] =>
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

  override type Request <: MessageAction

  def messageAction: MessageAction = request

  /** Java API */
  def getMessageAction: MessageAction = messageAction

}

object SqsAckResult {

  /**
   * Delete acknowledgment
   * @param request the delete message action
   * @param result the sqs DeleteMessageResponse
   */
  final class SqsDeleteResult @InternalApi private[sqs] (
      override val request: MessageAction.Delete,
      override val result: DeleteMessageResponse
  ) extends SqsAckResult {

    override type Request = MessageAction.Delete

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
   * @param request the ignore message action
   */
  final class SqsIgnoreResult @InternalApi private[sqs] (
      override val request: MessageAction.Ignore
  ) extends SqsAckResult {

    override type Request = MessageAction.Ignore

    override type Result = NotUsed.type

    override def responseMetadata: SqsResponseMetadata = SqsResult.EmptySqsResponseMetadata

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
   * @param request the change message visibility action
   * @param result the sqs ChangeMessageVisibilityResponse
   */
  final class SqsChangeMessageVisibilityResult @InternalApi private[sqs] (
      override val request: MessageAction.ChangeMessageVisibility,
      override val result: ChangeMessageVisibilityResponse
  ) extends SqsAckResult {

    override type Request = MessageAction.ChangeMessageVisibility

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
sealed abstract class SqsAckResultEntry extends SqsResultEntry {

  override type Request <: MessageAction

  def messageAction: MessageAction = request

  /** Java API */
  def getMessageAction: MessageAction = messageAction

}

object SqsAckResultEntry {

  /**
   * Delete acknowledgement within a batch
   * @param request the delete message action
   * @param result the sqs DeleteMessageBatchResultEntry
   */
  final class SqsDeleteResultEntry(override val request: MessageAction.Delete,
                                   override val result: DeleteMessageBatchResultEntry,
                                   override val responseMetadata: SqsResponseMetadata)
      extends SqsAckResultEntry {

    override type Request = MessageAction.Delete

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
   * @param request the ignore message action
   */
  final class SqsIgnoreResultEntry @InternalApi private[sqs] (
      override val request: MessageAction.Ignore
  ) extends SqsAckResultEntry {

    override type Request = MessageAction.Ignore

    override type Result = NotUsed.type

    override def responseMetadata: SqsResponseMetadata = SqsResult.EmptySqsResponseMetadata

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
   * @param request the change message visibility action
   * @param result the sqs ChangeMessageVisibilityBatchResultEntry
   */
  final class SqsChangeMessageVisibilityResultEntry(override val request: MessageAction.ChangeMessageVisibility,
                                                    override val result: ChangeMessageVisibilityBatchResultEntry,
                                                    override val responseMetadata: SqsResponseMetadata)
      extends SqsAckResultEntry {

    override type Request = MessageAction.ChangeMessageVisibility

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
