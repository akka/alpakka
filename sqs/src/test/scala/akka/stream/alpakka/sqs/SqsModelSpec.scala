/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import com.amazonaws.services.sqs.model.{Message, SendMessageResult}
import org.scalatest.{FlatSpec, Matchers}

class SqsModelSpec extends FlatSpec with Matchers {

  val msg = new Message()
  val otherMsg = new Message().withBody("other-body")

  "FifoMessageIdentifiers" should "implement proper equality" in {
    val sequenceNumber = "sequence-number"
    val otherSequenceNumber = "other-sequence-number"

    val messageGroupId = "group-id"
    val otherMessageGroupId = "other-group-id"

    val messageDeduplicationId = Option.empty[String]
    val otherMessageDeduplicationId = Some("deduplication-id")

    FifoMessageIdentifiers(sequenceNumber, messageGroupId, messageDeduplicationId) shouldBe FifoMessageIdentifiers(
      sequenceNumber,
      messageGroupId,
      messageDeduplicationId
    )
    FifoMessageIdentifiers(sequenceNumber, messageGroupId, messageDeduplicationId) should not be FifoMessageIdentifiers(
      otherSequenceNumber,
      messageGroupId,
      messageDeduplicationId
    )
    FifoMessageIdentifiers(sequenceNumber, messageGroupId, messageDeduplicationId) should not be FifoMessageIdentifiers(
      sequenceNumber,
      otherMessageGroupId,
      messageDeduplicationId
    )
    FifoMessageIdentifiers(sequenceNumber, messageGroupId, messageDeduplicationId) should not be FifoMessageIdentifiers(
      sequenceNumber,
      messageGroupId,
      otherMessageDeduplicationId
    )
  }

  "MessageAction.Delete" should "implement proper equality" in {
    MessageAction.Delete(msg) shouldBe MessageAction.Delete(msg)
    MessageAction.Delete(msg) should not be MessageAction.Delete(otherMsg)
  }

  "MessageAction.Ignore" should "implement proper equality" in {
    MessageAction.Ignore(msg) shouldBe MessageAction.Ignore(msg)
    MessageAction.Ignore(msg) should not be MessageAction.Ignore(otherMsg)
  }

  "MessageAction.ChangeMessageVisibility" should "implement proper equality" in {
    MessageAction.ChangeMessageVisibility(msg, 1) shouldBe MessageAction.ChangeMessageVisibility(msg, 1)
    MessageAction.ChangeMessageVisibility(msg, 1) should not be MessageAction.ChangeMessageVisibility(otherMsg, 1)
    MessageAction.ChangeMessageVisibility(msg, 1) should not be MessageAction.ChangeMessageVisibility(msg, 2)
  }

  it should "require valid visibility" in {
    a[IllegalArgumentException] should be thrownBy {
      MessageAction.ChangeMessageVisibility(msg, 43201)
    }
    a[IllegalArgumentException] should be thrownBy {
      MessageAction.ChangeMessageVisibility(msg, -1)
    }
  }

  it should "accept valid parameters" in {
    MessageAction.ChangeMessageVisibility(msg, 300)
  }

  it should "allow terminating visibility" in {
    MessageAction.ChangeMessageVisibility(msg, 0)
  }

  "SqsPublishResult" should "implement proper equality" in {
    val metadata = new SendMessageResult()
    val otherMetadata = new SendMessageResult().withMessageId("other-id")

    val fifoIdentifiers = Option.empty[FifoMessageIdentifiers]
    val otherFifoIdentifiers = Some(FifoMessageIdentifiers("sequence-number", "group-id", None))

    SqsPublishResult(metadata, msg, fifoIdentifiers) shouldBe SqsPublishResult(metadata, msg, fifoIdentifiers)
    SqsPublishResult(metadata, msg, fifoIdentifiers) should not be SqsPublishResult(otherMetadata, msg, fifoIdentifiers)
    SqsPublishResult(metadata, msg, fifoIdentifiers) should not be SqsPublishResult(metadata, otherMsg, fifoIdentifiers)
    SqsPublishResult(metadata, msg, fifoIdentifiers) should not be SqsPublishResult(metadata, msg, otherFifoIdentifiers)
  }

  "SqsAckResult" should "implement proper equality" in {
    val metadata = Some(new SendMessageResult())
    val otherMetadata = Some(new SendMessageResult().withMessageId("other-id"))

    val messageAction = MessageAction.Ignore(msg)
    val otherMessageAction = MessageAction.Ignore(otherMsg)

    SqsAckResult(metadata, messageAction) shouldBe SqsAckResult(metadata, messageAction)
    SqsAckResult(metadata, messageAction) should not be SqsAckResult(otherMetadata, messageAction)
    SqsAckResult(metadata, messageAction) should not be SqsAckResult(metadata, otherMessageAction)
  }
}
