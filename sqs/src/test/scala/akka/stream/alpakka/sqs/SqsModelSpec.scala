/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import akka.stream.alpakka.sqs.testkit.MessageFactory
import com.amazonaws.services.sqs.model.{Message, SendMessageResult}
import org.scalatest.{FlatSpec, Matchers}

class SqsModelSpec extends FlatSpec with Matchers {

  val msg = new Message()
  val otherMsg = new Message().withBody("other-body")

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

    MessageFactory.createFifoMessageIdentifiers(sequenceNumber, messageGroupId, messageDeduplicationId) shouldBe MessageFactory
      .createFifoMessageIdentifiers(
        sequenceNumber,
        messageGroupId,
        messageDeduplicationId
      )
    MessageFactory.createFifoMessageIdentifiers(sequenceNumber, messageGroupId, messageDeduplicationId) should not be MessageFactory
      .createFifoMessageIdentifiers(
        otherSequenceNumber,
        messageGroupId,
        messageDeduplicationId
      )
    MessageFactory.createFifoMessageIdentifiers(sequenceNumber, messageGroupId, messageDeduplicationId) should not be MessageFactory
      .createFifoMessageIdentifiers(
        sequenceNumber,
        otherMessageGroupId,
        messageDeduplicationId
      )
    MessageFactory.createFifoMessageIdentifiers(sequenceNumber, messageGroupId, messageDeduplicationId) should not be MessageFactory
      .createFifoMessageIdentifiers(
        sequenceNumber,
        messageGroupId,
        otherMessageDeduplicationId
      )
  }

  "SqsPublishResult" should "implement proper equality" in {
    val metadata = new SendMessageResult()
    val otherMetadata = new SendMessageResult().withMessageId("other-id")

    val fifoIdentifiers = Option.empty[FifoMessageIdentifiers]
    val otherFifoIdentifiers = Some(MessageFactory.createFifoMessageIdentifiers("sequence-number", "group-id", None))

    MessageFactory.createSqsPublishResult(metadata, msg, fifoIdentifiers) shouldBe MessageFactory
      .createSqsPublishResult(metadata, msg, fifoIdentifiers)
    MessageFactory.createSqsPublishResult(metadata, msg, fifoIdentifiers) should not be MessageFactory
      .createSqsPublishResult(otherMetadata, msg, fifoIdentifiers)
    MessageFactory.createSqsPublishResult(metadata, msg, fifoIdentifiers) should not be MessageFactory
      .createSqsPublishResult(metadata, otherMsg, fifoIdentifiers)
    MessageFactory.createSqsPublishResult(metadata, msg, fifoIdentifiers) should not be MessageFactory
      .createSqsPublishResult(metadata, msg, otherFifoIdentifiers)
  }

  "SqsAckResult" should "implement proper equality" in {
    val metadata = Some(new SendMessageResult())
    val otherMetadata = Some(new SendMessageResult().withMessageId("other-id"))

    val messageAction = MessageAction.Ignore(msg)
    val otherMessageAction = MessageAction.Ignore(otherMsg)

    MessageFactory.createSqsAckResult(metadata, messageAction) shouldBe MessageFactory.createSqsAckResult(metadata,
                                                                                                          messageAction)
    MessageFactory.createSqsAckResult(metadata, messageAction) should not be MessageFactory.createSqsAckResult(
      otherMetadata,
      messageAction
    )
    MessageFactory.createSqsAckResult(metadata, messageAction) should not be MessageFactory.createSqsAckResult(
      metadata,
      otherMessageAction
    )
  }
}
