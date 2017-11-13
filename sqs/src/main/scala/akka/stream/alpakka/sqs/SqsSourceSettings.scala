/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import java.util

import scala.collection.JavaConverters._

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

}

//#SqsSourceSettings
final case class SqsSourceSettings(
    waitTimeSeconds: Int,
    maxBufferSize: Int,
    maxBatchSize: Int,
    attributeNames: Seq[AttributeName] = Seq(),
    messageAttributeNames: Seq[MessageAttributeName] = Seq()
) {
  require(maxBatchSize <= maxBufferSize, "maxBatchSize must be lower or equal than maxBufferSize")
  // SQS requirements
  require(0 <= waitTimeSeconds && waitTimeSeconds <= 20,
          s"Invalid value ($waitTimeSeconds) for waitTimeSeconds. Requirement: 0 <= waitTimeSeconds <= 20 ")
  require(1 <= maxBatchSize && maxBatchSize <= 10,
          s"Invalid value ($maxBatchSize) for maxBatchSize. Requirement: 1 <= maxBatchSize <= 10 ")
}
//#SqsSourceSettings

final case class MessageAttributeName(name: String) {
  require(
    name.matches("[0-9a-zA-Z_\\-.]+"),
    "MessageAttributeNames may only contain alphanumeric characters and the underscore (_), hyphen (-), and period (.)"
  )

  require(
    !name.matches("(^\\.[^*].*)|(.*\\.\\..*)|(.*\\.$)"),
    "MessageAttributeNames cannot start or end with a period (.) or have multiple periods in succession (..)"
  )

  require(name.length <= 256, "MessageAttributeNames may not be longer than 256 characters")
}

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
