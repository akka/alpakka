/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import java.time.temporal.ChronoUnit

import scala.collection.immutable
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

final class SqsSourceSettings private (
    val waitTimeSeconds: Int,
    val maxBufferSize: Int,
    val maxBatchSize: Int,
    val attributeNames: immutable.Seq[AttributeName],
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

  /**
   * The maximum number of messages to return (see MaxNumberOfMessages in AWS docs).
   * Default: 10
   */
  def withMaxBatchSize(maxBatchSize: Int): SqsSourceSettings = copy(maxBatchSize = maxBatchSize)

  def withAttribute(attribute: AttributeName): SqsSourceSettings = copy(attributeNames = immutable.Seq(attribute))
  def withAttributes(attributes: immutable.Seq[AttributeName]): SqsSourceSettings = copy(attributeNames = attributes)

  /** Java API */
  def withAttributes(attributes: java.util.List[AttributeName]): SqsSourceSettings =
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
      maxBatchSize: Int = maxBatchSize,
      attributeNames: immutable.Seq[AttributeName] = attributeNames,
      messageAttributeNames: immutable.Seq[MessageAttributeName] = messageAttributeNames,
      closeOnEmptyReceive: Boolean = closeOnEmptyReceive,
      visibilityTimeout: Option[FiniteDuration] = visibilityTimeout
  ): SqsSourceSettings = new SqsSourceSettings(
    waitTimeSeconds,
    maxBufferSize,
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
    s"maxBatchSize=$maxBatchSize, " +
    s"attributeNames=${attributeNames.mkString(",")}, " +
    s"messageAttributeNames=${messageAttributeNames.mkString(",")}, " +
    s"closeOnEmptyReceive=$closeOnEmptyReceive," +
    s"visibilityTomeout=${visibilityTimeout.map(_.toCoarsest)}" +
    ")"
}

object SqsSourceSettings {
  val Defaults = new SqsSourceSettings(20,
                                       100,
                                       10,
                                       attributeNames = immutable.Seq(),
                                       messageAttributeNames = immutable.Seq(),
                                       closeOnEmptyReceive = false,
                                       visibilityTimeout = None)

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

/**
 * Source parameters as described at
 * https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html#API_ReceiveMessage_RequestParameters
 */
sealed abstract class AttributeName(val name: String)

case object All extends AttributeName("All")
case object ApproximateFirstReceiveTimestamp extends AttributeName("ApproximateFirstReceiveTimestamp")
case object ApproximateReceiveCount extends AttributeName("ApproximateReceiveCount")
case object SenderId extends AttributeName("SenderId")
case object SentTimestamp extends AttributeName("SentTimestamp")
case object MessageDeduplicationId extends AttributeName("MessageDeduplicationId")
case object MessageGroupId extends AttributeName("MessageGroupId")
case object SequenceNumber extends AttributeName("SequenceNumber")

case object Policy extends AttributeName("Policy")
case object VisibilityTimeout extends AttributeName("VisibilityTimeout")
case object MaximumMessageSize extends AttributeName("MaximumMessageSize")
case object MessageRetentionPeriod extends AttributeName("MessageRetentionPeriod")
case object ApproximateNumberOfMessages extends AttributeName("ApproximateNumberOfMessages")
case object ApproximateNumberOfMessagesNotVisible extends AttributeName("ApproximateNumberOfMessagesNotVisible")
case object CreatedTimestamp extends AttributeName("CreatedTimestamp")
case object LastModifiedTimestamp extends AttributeName("LastModifiedTimestamp")
case object QueueArn extends AttributeName("QueueArn")
case object ApproximateNumberOfMessagesDelayed extends AttributeName("ApproximateNumberOfMessagesDelayed")
case object DelaySeconds extends AttributeName("DelaySeconds")
case object ReceiveMessageWaitTimeSeconds extends AttributeName("ReceiveMessageWaitTimeSeconds")
case object RedrivePolicy extends AttributeName("RedrivePolicy")
case object FifoQueue extends AttributeName("FifoQueue")
case object ContentBasedDeduplication extends AttributeName("ContentBasedDeduplication")
case object KmsMasterKeyId extends AttributeName("KmsMasterKeyId")
case object KmsDataKeyReusePeriodSeconds extends AttributeName("KmsDataKeyReusePeriodSeconds")

/**
 * Java API:
 *
 * Source parameters as described at
 * https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html#API_ReceiveMessage_RequestParameters
 */
object Attribute {
  val all = All
  val approximateFirstReceiveTimestamp = ApproximateFirstReceiveTimestamp
  val approximateReceiveCount = ApproximateReceiveCount
  val senderId = SenderId
  val sentTimestamp = SentTimestamp
  val messageDeduplicationId = MessageDeduplicationId
  val messageGroupId = MessageGroupId
  val sequenceNumber = SequenceNumber

  val policy = Policy
  val visibilityTimeout = VisibilityTimeout
  val maximumMessageSize = MaximumMessageSize
  val messageRetentionPeriod = MessageRetentionPeriod
  val approximateNumberOfMessages = ApproximateNumberOfMessages
  val approximateNumberOfMessagesNotVisible = ApproximateNumberOfMessagesNotVisible
  val createdTimestamp = CreatedTimestamp
  val lastModifiedTimestamp = LastModifiedTimestamp
  val queueArn = QueueArn
  val approximateNumberOfMessagesDelayed = ApproximateNumberOfMessagesDelayed
  val delaySeconds = DelaySeconds
  val receiveMessageWaitTimeSeconds = ReceiveMessageWaitTimeSeconds
  val redrivePolicy = RedrivePolicy
  val fifoQueue = FifoQueue
  val contentBasedDeduplication = ContentBasedDeduplication
  val kmsMasterKeyId = KmsMasterKeyId
  val kmsDataKeyReusePeriodSeconds = KmsDataKeyReusePeriodSeconds
}
