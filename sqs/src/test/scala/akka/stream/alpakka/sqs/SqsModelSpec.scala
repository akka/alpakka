/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

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

  "SqsPublishResult" should "implement proper equality" in {
    val metadata = new SendMessageResult()
    val otherMetadata = new SendMessageResult().withMessageId("other-id")

    val body = "body"
    val otherBody = "other-body"

    SqsPublishResult(metadata, body) shouldBe SqsPublishResult(metadata, body)
    SqsPublishResult(metadata, body) should not be SqsPublishResult(otherMetadata, body)
    SqsPublishResult(metadata, body) should not be SqsPublishResult(metadata, otherBody)
  }

  "SqsAckResult" should "implement proper equality" in {
    val metadata = Some(new SendMessageResult())
    val otherMetadata = Some(new SendMessageResult().withMessageId("other-id"))

    val body = "body"
    val otherBody = "other-body"

    SqsAckResult(metadata, body) shouldBe SqsAckResult(metadata, body)
    SqsAckResult(metadata, body) should not be SqsAckResult(otherMetadata, body)
    SqsAckResult(metadata, body) should not be SqsAckResult(metadata, otherBody)
  }
}
