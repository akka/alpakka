/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import org.scalatest.{FlatSpec, Matchers}
import software.amazon.awssdk.services.sqs.model.{Message, SendMessageResponse}

class SqsModelSpec extends FlatSpec with Matchers {

  val msg = Message.builder().build()
  val otherMsg = Message.builder().body("other-body").build()

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

  "FifoMessageIdentifiers" should "implement proper equality" in {
    val sequenceNumber = "sequence-number"
    val otherSequenceNumber = "other-sequence-number"

    val messageGroupId = "group-id"
    val otherMessageGroupId = "other-group-id"

    val messageDeduplicationId = Option.empty[String]
    val otherMessageDeduplicationId = Some("deduplication-id")

    new FifoMessageIdentifiers(
      sequenceNumber,
      messageGroupId,
      messageDeduplicationId
    ) shouldBe new FifoMessageIdentifiers(
      sequenceNumber,
      messageGroupId,
      messageDeduplicationId
    )

    new FifoMessageIdentifiers(
      sequenceNumber,
      messageGroupId,
      messageDeduplicationId
    ) should not be new FifoMessageIdentifiers(
      otherSequenceNumber,
      messageGroupId,
      messageDeduplicationId
    )

    new FifoMessageIdentifiers(
      sequenceNumber,
      messageGroupId,
      messageDeduplicationId
    ) should not be new FifoMessageIdentifiers(
      sequenceNumber,
      otherMessageGroupId,
      messageDeduplicationId
    )

    new FifoMessageIdentifiers(
      sequenceNumber,
      messageGroupId,
      messageDeduplicationId
    ) should not be new FifoMessageIdentifiers(
      sequenceNumber,
      messageGroupId,
      otherMessageDeduplicationId
    )
  }

  "SqsPublishResult" should "implement proper equality" in {
    val metadata = SendMessageResponse.builder().build()
    val otherMetadata = SendMessageResponse.builder().messageId("other-id").build()

    val fifoIdentifiers = Option.empty[FifoMessageIdentifiers]
    val otherFifoIdentifiers = Some(new FifoMessageIdentifiers("sequence-number", "group-id", None))

    new SqsPublishResult(metadata, fifoIdentifiers) shouldBe new SqsPublishResult(metadata, fifoIdentifiers)
    new SqsPublishResult(otherMetadata, fifoIdentifiers) should not be new SqsPublishResult(metadata, fifoIdentifiers)
    new SqsPublishResult(metadata, otherFifoIdentifiers) should not be new SqsPublishResult(metadata, fifoIdentifiers)
  }

  "SqsAckResult" should "implement proper equality" in {
    val metadata = Some(SendMessageResponse.builder().build())
    val otherMetadata = Some(SendMessageResponse.builder().messageId("other-id").build())

    val messageAction = MessageAction.Ignore(msg)
    val otherMessageAction = MessageAction.Ignore(otherMsg)

    new SqsAckResult(metadata, messageAction) shouldBe new SqsAckResult(metadata, messageAction)
    new SqsAckResult(otherMetadata, messageAction) should not be new SqsAckResult(metadata, messageAction)
    new SqsAckResult(metadata, otherMessageAction) should not be new SqsAckResult(metadata, messageAction)
  }
}
