/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import akka.stream.alpakka.sqs.SqsAckResult._
import akka.stream.alpakka.sqs.SqsAckResultEntry._
import org.scalatest.{FlatSpec, Matchers}
import software.amazon.awssdk.awscore.DefaultAwsResponseMetadata
import software.amazon.awssdk.services.sqs.model._

class SqsModelSpec extends FlatSpec with Matchers {

  val msg = Message.builder().build()
  val otherMsg = Message.builder().body("other-body").build()
  val responseMetadata = SqsResponseMetadata.create(DefaultAwsResponseMetadata.create(java.util.Collections.emptyMap()))
  val otherResponseMetadata =
    SqsResponseMetadata.create(DefaultAwsResponseMetadata.create(java.util.Collections.singletonMap("k", "v")))

  "MessageAction.Delete" should "implement proper equality" in {
    MessageAction.Delete(msg) shouldBe MessageAction.Delete(msg)
    MessageAction.Delete(msg) should not be MessageAction.Delete(otherMsg)
    MessageAction.Delete(msg) should not be MessageAction.Ignore(otherMsg)
    MessageAction.Delete(msg) should not be MessageAction.ChangeMessageVisibility(otherMsg, 10)
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
    val request = SendMessageRequest.builder().messageBody(msg.body()).build()
    val otherRequest = SendMessageRequest.builder().messageBody(otherMsg.body()).build()

    val response = SendMessageResponse.builder().build()
    val otherResponse = SendMessageResponse.builder().messageId("1").build()

    val reference = new PublishResult(request, response)

    new PublishResult(request, response) shouldBe reference
    new PublishResult(otherRequest, response) should not be reference
    new PublishResult(request, otherResponse) should not be reference
  }

  "SqsPublishBatchResultEntry" should "implement proper equality" in {
    val request = SendMessageRequest.builder().messageBody(msg.body()).build()
    val otherRequest = SendMessageRequest.builder().messageBody(otherMsg.body()).build()

    val batchResultEntry = SendMessageBatchResultEntry.builder().build()
    val otherBatchResultEntry = SendMessageBatchResultEntry.builder().md5OfMessageBody("1234").build()

    val reference = new PublishResultEntry(request, batchResultEntry, responseMetadata)

    new PublishResultEntry(request, batchResultEntry, responseMetadata) shouldBe reference
    new PublishResultEntry(otherRequest, batchResultEntry, responseMetadata) should not be reference
    new PublishResultEntry(request, otherBatchResultEntry, responseMetadata) should not be reference
  }

  "DeleteResult" should "implement proper equality" in {
    val messageAction = MessageAction.Delete(msg)
    val otherMessageAction = MessageAction.Delete(otherMsg)

    val response = DeleteMessageResponse.builder().build() // there is only one possible response

    val reference = new DeleteResult(messageAction, response)

    new DeleteResult(messageAction, response) shouldBe reference
    new DeleteResult(otherMessageAction, response) should not be reference
  }

  "ChangeMessageVisibilityResult" should "implement proper equality" in {
    val messageAction = MessageAction.ChangeMessageVisibility(msg, 1)
    val otherMessageAction = MessageAction.ChangeMessageVisibility(otherMsg, 2)

    val response = ChangeMessageVisibilityResponse.builder().build() // there is only one possible response

    val reference = new ChangeMessageVisibilityResult(messageAction, response)

    new ChangeMessageVisibilityResult(messageAction, response) shouldBe reference
    new ChangeMessageVisibilityResult(otherMessageAction, response) should not be reference
  }

  "DeleteResultEntry" should "implement proper equality" in {
    val messageAction = MessageAction.Delete(msg)
    val otherMessageAction = MessageAction.Delete(otherMsg)

    val result = DeleteMessageBatchResultEntry.builder().build()
    val otherResult = DeleteMessageBatchResultEntry.builder().id("1").build()

    val reference = new DeleteResultEntry(messageAction, result, responseMetadata)

    new DeleteResultEntry(messageAction, result, responseMetadata) shouldBe reference
    new DeleteResultEntry(otherMessageAction, result, responseMetadata) should not be reference
    new DeleteResultEntry(messageAction, otherResult, responseMetadata) should not be reference
  }

  "ChangeMessageVisibilityResultEntry" should "implement proper equality" in {
    val messageAction = MessageAction.ChangeMessageVisibility(msg, 1)
    val otherMessageAction = MessageAction.ChangeMessageVisibility(otherMsg, 2)

    val result = ChangeMessageVisibilityBatchResultEntry.builder().build()
    val otherResult = ChangeMessageVisibilityBatchResultEntry.builder().id("1").build()

    val reference = new ChangeMessageVisibilityResultEntry(messageAction, result, responseMetadata)

    new ChangeMessageVisibilityResultEntry(messageAction, result, responseMetadata) shouldBe reference
    new ChangeMessageVisibilityResultEntry(messageAction, result, otherResponseMetadata) shouldBe reference // responseMetadata does not count in equality
    new ChangeMessageVisibilityResultEntry(otherMessageAction, result, responseMetadata) should not be reference
    new ChangeMessageVisibilityResultEntry(messageAction, otherResult, responseMetadata) should not be reference
  }
}
