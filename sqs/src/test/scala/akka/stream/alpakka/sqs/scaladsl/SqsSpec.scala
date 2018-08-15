/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import java.util.concurrent.CompletableFuture

import akka.Done
import akka.stream.alpakka.sqs._
import akka.stream.scaladsl.{Sink, Source}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model._
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{spy, times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.mockito.MockitoSugar.mock
import org.scalatest.{FlatSpec, Matchers}

class SqsSpec extends FlatSpec with Matchers with DefaultTestContext {

  private val sqsSourceSettings = SqsSourceSettings.Defaults

  it should "pull a message and publish it to another queue" taggedAs Integration in {
    val queue1 = randomQueueUrl()
    val queue2 = randomQueueUrl()
    implicit val awsSqsClient = sqsClient

    sqsClient.sendMessage(queue1, "alpakka")

    val future = SqsSource(queue1, sqsSourceSettings)
      .take(1)
      .map { m: Message =>
        m.getBody
      }
      .runWith(SqsSink(queue2))
    future.futureValue shouldBe Done

    val result = sqsClient.receiveMessage(queue2)
    result.getMessages.size() shouldBe 1
    result.getMessages.get(0).getBody shouldBe "alpakka"
  }

  it should "delete all messages in batches of given size" taggedAs Integration in {
    val queue = randomQueueUrl()
    implicit val awsSqsClient = spy(sqsClient)

    val messages = for (i <- 0 until 10) yield new SendMessageRequest().withMessageBody(s"Message - $i")

    val future1 = Source(messages).via(SqsFlow(queue)).runWith(Sink.ignore)
    future1.futureValue shouldBe Done

    val future = SqsSource(queue)
      .take(10)
      .map { m: Message =>
        MessageAction.Delete(m)
      }
      .via(SqsAckFlow.grouped(queue, SqsBatchAckFlowSettings().withMaxBatchSize(5)))
      .runWith(Sink.ignore)

    future.futureValue shouldBe Done
    verify(awsSqsClient, times(2)).deleteMessageBatchAsync(
      any[DeleteMessageBatchRequest],
      any[AsyncHandler[DeleteMessageBatchRequest, DeleteMessageBatchResult]]
    )
  }

  it should "fail if any of the messages in the batch request failed" in {
    val messages = for (i <- 0 until 10) yield new Message().withBody(s"Message - $i")

    implicit val mockAwsSqsClient = mock[AmazonSQSAsync]
    when(
      mockAwsSqsClient.deleteMessageBatchAsync(any[DeleteMessageBatchRequest],
                                               any[AsyncHandler[DeleteMessageBatchRequest, DeleteMessageBatchResult]])
    ).thenAnswer(new Answer[AnyRef] {
      override def answer(invocation: InvocationOnMock): AnyRef = {
        val request = invocation.getArgument[DeleteMessageBatchRequest](0)
        invocation
          .getArgument[AsyncHandler[DeleteMessageBatchRequest, DeleteMessageBatchResult]](1)
          .onSuccess(request, new DeleteMessageBatchResult().withFailed(new BatchResultErrorEntry()))
        new CompletableFuture[DeleteMessageBatchRequest]
      }
    })

    val future = Source(messages)
      .take(10)
      .map { m: Message =>
        MessageAction.Delete(m)
      }
      .via(SqsAckFlow.grouped("queue", SqsBatchAckFlowSettings.Defaults))
      .runWith(Sink.ignore)
    future.failed.futureValue shouldBe a[BatchException]
  }

  it should "fail if the batch request failed" in {
    val messages = for (i <- 0 until 10) yield new Message().withBody(s"Message - $i")

    implicit val mockAwsSqsClient = mock[AmazonSQSAsync]
    when(
      mockAwsSqsClient.deleteMessageBatchAsync(any[DeleteMessageBatchRequest],
                                               any[AsyncHandler[DeleteMessageBatchRequest, DeleteMessageBatchResult]])
    ).thenAnswer(new Answer[AnyRef] {
      override def answer(invocation: InvocationOnMock): AnyRef = {
        invocation
          .getArgument[AsyncHandler[DeleteMessageBatchRequest, DeleteMessageBatchResult]](1)
          .onError(new RuntimeException("error"))
        new CompletableFuture[DeleteMessageBatchRequest]
      }
    })

    val future = Source(messages)
      .take(10)
      .map { m: Message =>
        MessageAction.Delete(m)
      }
      .via(SqsAckFlow.grouped("queue", SqsBatchAckFlowSettings.Defaults))
      .runWith(Sink.ignore)

    future.failed.futureValue shouldBe a[BatchException]
  }

  it should "fail if the client invocation failed" in {
    val messages = for (i <- 0 until 10) yield new Message().withBody(s"Message - $i")

    implicit val mockAwsSqsClient = mock[AmazonSQSAsync]
    when(
      mockAwsSqsClient.deleteMessageBatchAsync(any[DeleteMessageBatchRequest],
                                               any[AsyncHandler[DeleteMessageBatchRequest, DeleteMessageBatchResult]])
    ).thenThrow(new RuntimeException("error"))

    val future = Source(messages)
      .take(10)
      .map { m: Message =>
        MessageAction.Delete(m)
      }
      .via(SqsAckFlow.grouped("queue", SqsBatchAckFlowSettings.Defaults))
      .runWith(Sink.ignore)

    future.failed.futureValue shouldBe a[RuntimeException]
  }

  it should "delete, delay & ignore all messages in batches of given size" in {
    val queue = randomQueueUrl()
    implicit val awsSqsClient = spy(sqsClient)

    val messages = for (i <- 0 until 10) yield new SendMessageRequest().withMessageBody(s"Message - $i")

    val future1 = Source(messages).via(SqsFlow(queue)).runWith(Sink.ignore)
    future1.futureValue shouldBe Done

    var i = 0
    val future = SqsSource(queue)
      .take(10)
      .map { m: Message =>
        val msg =
          if (i % 3 == 0)
            MessageAction.Delete(m)
          else if (i % 3 == 1)
            MessageAction.ChangeMessageVisibility(m, 5)
          else
            MessageAction.Ignore(m)
        i += 1
        msg
      }
      .via(SqsAckFlow.grouped(queue, SqsBatchAckFlowSettings.Defaults))
      .runWith(Sink.ignore)

    future.futureValue shouldBe Done
    verify(awsSqsClient, times(1)).deleteMessageBatchAsync(
      any[DeleteMessageBatchRequest],
      any[AsyncHandler[DeleteMessageBatchRequest, DeleteMessageBatchResult]]
    )
    verify(awsSqsClient, times(1))
      .changeMessageVisibilityBatchAsync(
        any[ChangeMessageVisibilityBatchRequest],
        any[AsyncHandler[ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityBatchResult]]
      )
  }
}
