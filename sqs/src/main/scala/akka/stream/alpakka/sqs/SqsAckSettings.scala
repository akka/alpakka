/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import scala.concurrent.duration._

object SqsAckSinkSettings {
  val Defaults = SqsAckSinkSettings(maxInFlight = 10)
}

//#SqsAckSinkSettings
final case class SqsAckSinkSettings(maxInFlight: Int) {
  require(maxInFlight > 0)
}
//#SqsAckSinkSettings

object SqsBatchAckFlowSettings {
  val Defaults = SqsBatchAckFlowSettings(
    maxBatchSize = 10,
    maxBatchWait = 500.millis,
    concurrentRequests = 1
  )
}

//#SqsBatchAckFlowSettings
final case class SqsBatchAckFlowSettings(maxBatchSize: Int, maxBatchWait: FiniteDuration, concurrentRequests: Int) {
  require(concurrentRequests > 0)
  require(
    maxBatchSize > 0 && maxBatchSize <= 10,
    s"Invalid value for maxBatchSize: $maxBatchSize. It should be 0 < maxBatchSize < 10, due to the Amazon SQS requirements."
  )
  def withMaxBatchSize(maxBatchSize: Int): SqsBatchAckFlowSettings = this.copy(maxBatchSize = maxBatchSize)
  def withMaxBatchWait(maxBatchWait: FiniteDuration): SqsBatchAckFlowSettings = this.copy(maxBatchWait = maxBatchWait)
  def withConcurrentRequests(concurrentRequests: Int): SqsBatchAckFlowSettings =
    this.copy(concurrentRequests = concurrentRequests)
}
//#SqsBatchAckFlowSettings

sealed abstract class MessageAction

object Delete {
  @deprecated("Use `MessageAction.Delete` instead", "0.15")
  def apply(): MessageAction = MessageAction.Delete
}

object Ignore {
  @deprecated("Use `MessageAction.Ignore` instead", "0.15")
  def apply(): MessageAction = MessageAction.Ignore
}

object ChangeMessageVisibility {
  @deprecated("Use `MessageAction.ChangeMessageVisibility` instead", "0.15")
  def apply(visibilityTimeout: Int): MessageAction = MessageAction.ChangeMessageVisibility(visibilityTimeout)
}

object MessageAction {

  /**
   * Delete the message from the queue.
   *
   * @see [https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html DeleteMessage]
   */
  final case object Delete extends MessageAction

  /**
   * Ignore the message.
   */
  final case object Ignore extends MessageAction

  /**
   * Change the visibility timeout of the message.
   * The maximum allowed timeout value is 12 hours.
   *
   * @param visibilityTimeout new timeout in seconds
   *
   * @see [https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ChangeMessageVisibility.html ChangeMessageVisibility]
   */
  final case class ChangeMessageVisibility(visibilityTimeout: Int) extends MessageAction {
    // SQS requirements
    require(
      0 <= visibilityTimeout && visibilityTimeout <= 43200,
      s"Invalid value ($visibilityTimeout) for visibilityTimeout. Requirement: 0 <= waitTimeSeconds <= 43200"
    )
  }

  /**
   * Java API: Delete the message from the queue.
   */
  def delete: MessageAction = Delete

  /**
   * Java API: Ignore the message.
   */
  def ignore: MessageAction = Ignore

  /**
   * Java API: Change the visibility timeout of the message.
   * @param visibilityTimeout new timeout in seconds
   */
  def changeMessageVisibility(visibilityTimeout: Int): MessageAction = ChangeMessageVisibility(visibilityTimeout)

}
