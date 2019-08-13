/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import java.time.temporal.ChronoUnit

import software.amazon.awssdk.services.sqs.model

import scala.collection.immutable
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

final class SqsSourceSettings private (
    val waitTimeSeconds: Int,
    val maxBufferSize: Int,
    val parallelRequests: Int,
    val maxBatchSize: Int,
    val attributeNames: immutable.Seq[MessageSystemAttributeName],
    val messageAttributeNames: immutable.Seq[MessageAttributeName],
    val closeOnEmptyReceive: Boolean,
    val visibilityTimeout: Option[FiniteDuration]
) {
  require(maxBatchSize <= maxBufferSize, "maxBatchSize must be lower or equal than maxBufferSize")
  // SQS requirements
  require(0 <= waitTimeSeconds && waitTimeSeconds <= 20,
          s"Invalid value ($waitTimeSeconds) for waitTimeSeconds. Requirement: 0 <= waitTimeSeconds <= 20 ")
  require(1 <= maxBatchSize && maxBatchSize <= 10,
          s"Invalid value ($maxBatchSize) for maxBatchSize. Requirement: 1 <= maxBatchSize <= 10 ")

  /**
   * The duration in seconds for which the call waits for a message to arrive in the queue before returning.
   * (see WaitTimeSeconds in AWS docs).
   * Default: 20 seconds
   */
  def withWaitTimeSeconds(seconds: Int): SqsSourceSettings = copy(waitTimeSeconds = seconds)

  /**
   * The duration for which the call waits for a message to arrive in the queue before returning.
   * (see WaitTimeSeconds in AWS docs).
   *
   * Default: 20 seconds
   */
  def withWaitTime(duration: FiniteDuration): SqsSourceSettings = copy(waitTimeSeconds = duration.toSeconds.toInt)

  /**
   * Java API
   *
   * The duration in seconds for which the call waits for a message to arrive in the queue before returning.
   * (see WaitTimeSeconds in AWS docs).
   *
   *  Default: 20 seconds
   */
  def withWaitTime(duration: java.time.Duration): SqsSourceSettings =
    copy(waitTimeSeconds = duration.get(ChronoUnit.SECONDS).toInt)

  /**
   * Internal buffer size used by the Source.
   *
   * Default: 100 messages
   */
  def withMaxBufferSize(maxBufferSize: Int): SqsSourceSettings = copy(maxBufferSize = maxBufferSize)

  def withParallelRequests(value: Int): SqsSourceSettings = copy(parallelRequests = value)

  /**
   * The maximum number of messages to return (see MaxNumberOfMessages in AWS docs).
   * Default: 10
   */
  def withMaxBatchSize(maxBatchSize: Int): SqsSourceSettings = copy(maxBatchSize = maxBatchSize)

  def withAttribute(attribute: MessageSystemAttributeName): SqsSourceSettings =
    copy(attributeNames = immutable.Seq(attribute))
  def withAttributes(attributes: immutable.Seq[MessageSystemAttributeName]): SqsSourceSettings =
    copy(attributeNames = attributes)

  /** Java API */
  def withAttributes(attributes: java.util.List[MessageSystemAttributeName]): SqsSourceSettings =
    copy(attributeNames = attributes.asScala.toList)

  def withMessageAttribute(attributes: MessageAttributeName): SqsSourceSettings =
    copy(messageAttributeNames = immutable.Seq(attributes))
  def withMessageAttributes(attributes: immutable.Seq[MessageAttributeName]): SqsSourceSettings =
    copy(messageAttributeNames = attributes)

  /** Java API */
  def withMessageAttributes(attributes: java.util.List[MessageAttributeName]): SqsSourceSettings =
    copy(messageAttributeNames = attributes.asScala.toList)

  /**
   * If true, the source completes when no messages are available.
   *
   * Default: false
   */
  def withCloseOnEmptyReceive(value: Boolean): SqsSourceSettings =
    if (value == closeOnEmptyReceive) this
    else copy(closeOnEmptyReceive = value)

  /**
   * the period of time (in seconds) during which Amazon SQS prevents other consumers
   * from receiving and processing an already received message (see Amazon SQS doc)
   *
   * Default: None - taken from the SQS queue configuration
   */
  def withVisibilityTimeout(timeout: FiniteDuration): SqsSourceSettings =
    copy(visibilityTimeout = Some(timeout))

  private def copy(
      waitTimeSeconds: Int = waitTimeSeconds,
      maxBufferSize: Int = maxBufferSize,
      parallelRequests: Int = parallelRequests,
      maxBatchSize: Int = maxBatchSize,
      attributeNames: immutable.Seq[MessageSystemAttributeName] = attributeNames,
      messageAttributeNames: immutable.Seq[MessageAttributeName] = messageAttributeNames,
      closeOnEmptyReceive: Boolean = closeOnEmptyReceive,
      visibilityTimeout: Option[FiniteDuration] = visibilityTimeout
  ): SqsSourceSettings = new SqsSourceSettings(
    waitTimeSeconds,
    maxBufferSize,
    parallelRequests,
    maxBatchSize,
    attributeNames,
    messageAttributeNames,
    closeOnEmptyReceive,
    visibilityTimeout
  )

  override def toString: String =
    "SqsSourceSettings(" +
    s"waitTimeSeconds=$waitTimeSeconds, " +
    s"maxBufferSize=$maxBufferSize, " +
    s"parallelRequests=$parallelRequests, " +
    s"maxBatchSize=$maxBatchSize, " +
    s"attributeNames=${attributeNames.mkString(",")}, " +
    s"messageAttributeNames=${messageAttributeNames.mkString(",")}, " +
    s"closeOnEmptyReceive=$closeOnEmptyReceive," +
    s"visibilityTomeout=${visibilityTimeout.map(_.toCoarsest)}" +
    ")"
}

object SqsSourceSettings {
  val Defaults = new SqsSourceSettings(
    waitTimeSeconds = 20,
    maxBufferSize = 100,
    parallelRequests = 1,
    maxBatchSize = 10,
    attributeNames = immutable.Seq.empty,
    messageAttributeNames = immutable.Seq.empty,
    closeOnEmptyReceive = false,
    visibilityTimeout = None
  )

  /**
   * Scala API
   */
  def apply(): SqsSourceSettings = Defaults

  /**
   * Java API
   */
  def create(): SqsSourceSettings = Defaults
}

/**
 * Message attribure names described at
 * https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html#API_ReceiveMessage_RequestParameters
 */
final class MessageAttributeName private (val name: String) {
  require(
    name.matches("[0-9a-zA-Z_\\-.*]+"),
    "MessageAttributeNames may only contain alphanumeric characters and the underscore (_), hyphen (-), period (.), or star (*)"
  )

  require(
    !name.matches("(^\\.[^*].*)|(.*\\.\\..*)|(.*\\.$)"),
    "MessageAttributeNames cannot start or end with a period (.) or have multiple periods in succession (..)"
  )

  require(name.length <= 256, "MessageAttributeNames may not be longer than 256 characters")

  def getName: String = name

  override def toString: String = s"MessageAttributeName($name)"

}

object MessageAttributeName {

  /**
   * Scala API:
   * Create an instance containing `name`
   */
  def apply(name: String): MessageAttributeName = new MessageAttributeName(name)

  /**
   * Java API:
   * Create an instance containing `name`
   */
  def create(name: String): MessageAttributeName = new MessageAttributeName(name)
}

sealed abstract class AttributeName(val name: String)

/**
 * Source parameters as described at
 * https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html#API_ReceiveMessage_RequestParameters
 */
sealed abstract class MessageSystemAttributeName(_name: String) extends AttributeName(_name) {
  protected def this(messageSystemAttributeName: model.MessageSystemAttributeName) {
    this(messageSystemAttributeName.toString)
  }
}

// All is missing in [[software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName]]
// (see https://github.com/akka/alpakka/pull/1839)
case object All extends MessageSystemAttributeName("All")

case object ApproximateFirstReceiveTimestamp
    extends MessageSystemAttributeName(model.MessageSystemAttributeName.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP)
case object ApproximateReceiveCount
    extends MessageSystemAttributeName(model.MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT)
case object SenderId extends MessageSystemAttributeName(model.MessageSystemAttributeName.SENDER_ID)
case object SentTimestamp extends MessageSystemAttributeName(model.MessageSystemAttributeName.SENT_TIMESTAMP)
case object MessageDeduplicationId
    extends MessageSystemAttributeName(model.MessageSystemAttributeName.MESSAGE_DEDUPLICATION_ID)
case object MessageGroupId extends MessageSystemAttributeName(model.MessageSystemAttributeName.MESSAGE_GROUP_ID)
case object SequenceNumber extends MessageSystemAttributeName(model.MessageSystemAttributeName.SEQUENCE_NUMBER)

/**
 * Source parameters as described at
 * https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_GetQueueAttributes.html
 */
sealed abstract class QueueAttributeName(queueAttributeName: model.QueueAttributeName)
    extends AttributeName(queueAttributeName.toString)

case object Policy extends QueueAttributeName(model.QueueAttributeName.POLICY)
case object VisibilityTimeout extends QueueAttributeName(model.QueueAttributeName.VISIBILITY_TIMEOUT)
case object MaximumMessageSize extends QueueAttributeName(model.QueueAttributeName.MAXIMUM_MESSAGE_SIZE)
case object MessageRetentionPeriod extends QueueAttributeName(model.QueueAttributeName.MESSAGE_RETENTION_PERIOD)
case object ApproximateNumberOfMessages
    extends QueueAttributeName(model.QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
case object ApproximateNumberOfMessagesNotVisible
    extends QueueAttributeName(model.QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)
case object CreatedTimestamp extends QueueAttributeName(model.QueueAttributeName.CREATED_TIMESTAMP)
case object LastModifiedTimestamp extends QueueAttributeName(model.QueueAttributeName.LAST_MODIFIED_TIMESTAMP)
case object QueueArn extends QueueAttributeName(model.QueueAttributeName.QUEUE_ARN)
case object ApproximateNumberOfMessagesDelayed
    extends QueueAttributeName(model.QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED)
case object DelaySeconds extends QueueAttributeName(model.QueueAttributeName.DELAY_SECONDS)
case object ReceiveMessageWaitTimeSeconds
    extends QueueAttributeName(model.QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS)
case object RedrivePolicy extends QueueAttributeName(model.QueueAttributeName.REDRIVE_POLICY)
case object FifoQueue extends QueueAttributeName(model.QueueAttributeName.FIFO_QUEUE)
case object ContentBasedDeduplication extends QueueAttributeName(model.QueueAttributeName.CONTENT_BASED_DEDUPLICATION)
case object KmsMasterKeyId extends QueueAttributeName(model.QueueAttributeName.KMS_MASTER_KEY_ID)
case object KmsDataKeyReusePeriodSeconds
    extends QueueAttributeName(model.QueueAttributeName.KMS_DATA_KEY_REUSE_PERIOD_SECONDS)

/**
 * Java API:
 *
 * Source parameters as described at
 * https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html#API_ReceiveMessage_RequestParameters
 */
object MessageSystemAttributeName {
  val all = All
  val approximateFirstReceiveTimestamp = ApproximateFirstReceiveTimestamp
  val approximateReceiveCount = ApproximateReceiveCount
  val senderId = SenderId
  val sentTimestamp = SentTimestamp
  val messageDeduplicationId = MessageDeduplicationId
  val messageGroupId = MessageGroupId
  val sequenceNumber = SequenceNumber
}
