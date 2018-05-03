/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import java.util

import scala.annotation.varargs
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

object SqsSourceSettings {
  val Defaults = SqsSourceSettings(20, 100, 10)

  def create(waitTimeSeconds: Int, maxBufferSize: Int, maxBatchSize: Int): SqsSourceSettings =
    SqsSourceSettings(waitTimeSeconds, maxBufferSize, maxBatchSize)

  def create(waitTimeSeconds: Int,
             maxBufferSize: Int,
             maxBatchSize: Int,
             attributeNames: util.List[AttributeName],
             messageAttributeNames: util.List[MessageAttributeName]): SqsSourceSettings =
    SqsSourceSettings(waitTimeSeconds,
                      maxBufferSize,
                      maxBatchSize,
                      attributeNames.asScala,
                      messageAttributeNames.asScala)

  def create(waitTimeSeconds: Int,
             maxBufferSize: Int,
             maxBatchSize: Int,
             attributeNames: util.List[AttributeName],
             messageAttributeNames: util.List[MessageAttributeName],
             closeOnEmptyReceive: Boolean): SqsSourceSettings =
    SqsSourceSettings(waitTimeSeconds,
                      maxBufferSize,
                      maxBatchSize,
                      attributeNames.asScala,
                      messageAttributeNames.asScala,
                      closeOnEmptyReceive)

}

final case class SqsSourceSettings(
    waitTimeSeconds: Int,
    maxBufferSize: Int,
    maxBatchSize: Int,
    attributeNames: Seq[AttributeName] = Seq(),
    messageAttributeNames: Seq[MessageAttributeName] = Seq(),
    closeOnEmptyReceive: Boolean = false
) {
  require(maxBatchSize <= maxBufferSize, "maxBatchSize must be lower or equal than maxBufferSize")
  // SQS requirements
  require(0 <= waitTimeSeconds && waitTimeSeconds <= 20,
          s"Invalid value ($waitTimeSeconds) for waitTimeSeconds. Requirement: 0 <= waitTimeSeconds <= 20 ")
  require(1 <= maxBatchSize && maxBatchSize <= 10,
          s"Invalid value ($maxBatchSize) for maxBatchSize. Requirement: 1 <= maxBatchSize <= 10 ")

  def withWaitTimeSeconds(seconds: Int): SqsSourceSettings = copy(waitTimeSeconds = seconds)

  def withWaitTime(duration: FiniteDuration): SqsSourceSettings = copy(waitTimeSeconds = duration.toSeconds.toInt)

  def withMaxBufferSize(maxBufferSize: Int): SqsSourceSettings = copy(maxBufferSize = maxBufferSize)

  def withMaxBatchSize(maxBatchSize: Int): SqsSourceSettings = copy(maxBatchSize = maxBatchSize)

  @varargs
  def withAttributes(attributes: AttributeName*): SqsSourceSettings = copy(attributeNames = attributes)

  @varargs
  def withMessageAttributes(attributes: MessageAttributeName*): SqsSourceSettings =
    copy(messageAttributeNames = attributes)

  def withCloseOnEmptyReceive(): SqsSourceSettings = copy(closeOnEmptyReceive = true)

  def withoutCloseOnEmptyReceive(): SqsSourceSettings = copy(closeOnEmptyReceive = false)
}

/**
 * Message attribure names described at
 * https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html#API_ReceiveMessage_RequestParameters
 */
final case class MessageAttributeName(name: String) {
  require(
    name.matches("[0-9a-zA-Z_\\-.*]+"),
    "MessageAttributeNames may only contain alphanumeric characters and the underscore (_), hyphen (-), period (.), or star (*)"
  )

  require(
    !name.matches("(^\\.[^*].*)|(.*\\.\\..*)|(.*\\.$)"),
    "MessageAttributeNames cannot start or end with a period (.) or have multiple periods in succession (..)"
  )

  require(name.length <= 256, "MessageAttributeNames may not be longer than 256 characters")

}

object MessageAttributeName {

  /**
   * Java API:
   * Create an instance containing `name`
   */
  def create(name: String): MessageAttributeName = MessageAttributeName(name)
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
