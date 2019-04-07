/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import java.util.Collections

import org.scalatest.{FlatSpec, Matchers}
import software.amazon.awssdk.awscore.AwsResponseMetadata
import software.amazon.awssdk.services.sqs.model.{Message, SendMessageRequest, SendMessageResponse, SqsResponseMetadata}

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

  trait Fixture {
    private object TestResponseMetadata extends AwsResponseMetadata(Collections.emptyMap[String, String]())

    val responseMetadata = SqsResponseMetadata.create(TestResponseMetadata)
    val otherResponseMetadata = SqsResponseMetadata.create(TestResponseMetadata)
    val metadata = SendMessageResponse.builder().build()
    val otherMetadata = SendMessageResponse.builder().messageId("other-id").build()
  }

  "SqsPublishResult" should "implement proper equality" in new Fixture {
    val request = SendMessageRequest.builder().build()
    val otherRequest = SendMessageRequest.builder().messageBody("other-body").build()

    val reference = new SqsPublishResult(responseMetadata, metadata, request)

    new SqsPublishResult(responseMetadata, metadata, request) shouldBe reference
    new SqsPublishResult(otherResponseMetadata, metadata, request) should not be reference
    new SqsPublishResult(responseMetadata, otherMetadata, request) should not be reference
    new SqsPublishResult(responseMetadata, metadata, otherRequest) should not be reference
  }

  "SqsAckResult" should "implement proper equality" in new Fixture {
    val messageAction = MessageAction.Ignore(msg)
    val otherMessageAction = MessageAction.Ignore(otherMsg)

    val reference = new SqsAckResult(responseMetadata, metadata, messageAction)

    new SqsAckResult(responseMetadata, metadata, messageAction) shouldBe reference
    new SqsAckResult(otherResponseMetadata, metadata, messageAction) should not be reference
    new SqsAckResult(responseMetadata, otherMetadata, messageAction) should not be reference
    new SqsAckResult(responseMetadata, metadata, otherMessageAction) should not be reference
  }
}
