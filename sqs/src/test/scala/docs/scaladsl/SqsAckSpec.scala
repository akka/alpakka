/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.util.concurrent.CompletableFuture

import akka.Done
import akka.stream.alpakka.sqs._
import akka.stream.alpakka.sqs.scaladsl._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model._
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{spy, times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.mockito.MockitoSugar.mock
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class SqsAckSpec extends FlatSpec with Matchers with DefaultTestContext {

  "SqsAckSettings" should "construct settings" in {
    //#SqsAckSettings
    val sinkSettings =
      SqsAckSettings()
        .withMaxInFlight(10)
    //#SqsAckSettings
    sinkSettings.maxInFlight should be(10)
  }

  it should "require valid maxInFlight" in {
    a[IllegalArgumentException] should be thrownBy {
      SqsAckSettings(0)
    }
  }

  it should "accept valid parameters" in {
    SqsAckSettings(1)
  }

  "SqsAckBatchSettings" should "construct settings" in {
    //#SqsAckBatchSettings
    val batchSettings =
      SqsAckBatchSettings()
        .withConcurrentRequests(1)
    //#SqsAckBatchSettings
    batchSettings.concurrentRequests should be(1)
  }

  "SqsAckGroupedSettings" should "construct settings" in {
    //#SqsAckGroupedSettings
    val batchSettings =
      SqsAckGroupedSettings()
        .withMaxBatchSize(10)
        .withMaxBatchWait(500.millis)
        .withConcurrentRequests(1)
    //#SqsAckGroupedSettings
    batchSettings.maxBatchSize should be(10)
  }

  "AckSink" should "pull and delete message" taggedAs Integration in {
    val queue = randomQueueUrl()
    sqsClient.sendMessage(queue, "alpakka-2")
    implicit val awsSqsClient = spy(sqsClient)

    val future =
      //#ack
      SqsSource(queue)
        .take(1)
        .map(MessageAction.Delete(_))
        .runWith(SqsAckSink(queue))
    //#ack

    future.futureValue shouldBe Done
    verify(awsSqsClient).deleteMessageAsync(any[DeleteMessageRequest],
                                            any[AsyncHandler[DeleteMessageRequest, DeleteMessageResult]])
  }

  it should "pull and delay a message" taggedAs Integration in {
    val queue = randomQueueUrl()
    sqsClient.sendMessage(queue, "alpakka-3")

    implicit val awsSqsClient = spy(sqsClient)
    //#requeue
    val future = SqsSource(queue)
      .take(1)
      .map(MessageAction.ChangeMessageVisibility(_, 5))
      .runWith(SqsAckSink(queue))
    //#requeue

    Await.result(future, 1.second) shouldBe Done
    verify(awsSqsClient).changeMessageVisibilityAsync(
      any[ChangeMessageVisibilityRequest],
      any[AsyncHandler[ChangeMessageVisibilityRequest, ChangeMessageVisibilityResult]]
    )
  }

  it should "pull and ignore a message" taggedAs Integration in {
    val queue = randomQueueUrl()
    sqsClient.sendMessage(queue, "alpakka-4")

    implicit val awsSqsClient = spy(sqsClient)
    //#ignore
    SqsSource(queue)
      .map(MessageAction.Ignore(_))
      .runWith(SqsAckSink(queue))
    //#ignore

    // TODO: assertions missing
  }

  "AckFlow" should "pull and delete message via flow" taggedAs Integration in {
    val queue = randomQueueUrl()
    sqsClient.sendMessage(queue, "alpakka-flow-ack")

    implicit val awsSqsClient = spy(sqsClient)
    val future =
      //#flow-ack
      SqsSource(queue)
        .take(1)
        .map(MessageAction.Delete(_))
        .via(SqsAckFlow(queue))
        .runWith(Sink.ignore)
    //#flow-ack

    future.futureValue shouldBe Done
    verify(awsSqsClient).deleteMessageAsync(any[DeleteMessageRequest],
                                            any[AsyncHandler[DeleteMessageRequest, DeleteMessageResult]])
  }

  it should "pull and ignore a message" taggedAs Integration in {
    val queue = randomQueueUrl()
    sqsClient.sendMessage(queue, "alpakka-4")

    implicit val awsSqsClient = spy(sqsClient)
    val result =
      //#ignore
      SqsSource(queue)
        .take(1)
        .map(MessageAction.Ignore(_))
        .via(SqsAckFlow(queue))
        //#ignore
        .runWith(TestSink.probe[SqsAckResult])
        .requestNext(1.second)

    result.metadata shouldBe empty
    result.message shouldBe "alpakka-4"
  }

  it should "delete batch of messages" taggedAs Integration in {
    val queue = randomQueueUrl()
    implicit val awsSqsClient = spy(sqsClient)

    val messages = for (i <- 0 until 10) yield new SendMessageRequest().withMessageBody(s"Message - $i")

    val future1 = Source(messages).via(SqsPublishFlow(queue)).runWith(Sink.ignore)
    future1.futureValue shouldBe Done

    val future =
      //#batch-ack
      SqsSource(queue)
        .take(10)
        .map(MessageAction.Delete(_))
        .via(SqsAckFlow.grouped(queue, SqsAckGroupedSettings.Defaults))
        .runWith(Sink.ignore)
    //#batch-ack

    future.futureValue shouldBe Done
    verify(awsSqsClient, times(1)).deleteMessageBatchAsync(
      any[DeleteMessageBatchRequest],
      any[AsyncHandler[DeleteMessageBatchRequest, DeleteMessageBatchResult]]
    )
  }

  it should "delete all messages in batches of given size" taggedAs Integration in {
    val queue = randomQueueUrl()
    implicit val awsSqsClient = spy(sqsClient)

    val messages = for (i <- 0 until 10) yield new SendMessageRequest().withMessageBody(s"Message - $i")

    val future1 = Source(messages).via(SqsPublishFlow(queue)).runWith(Sink.ignore)
    future1.futureValue shouldBe Done

    val future = SqsSource(queue)
      .take(10)
      .map(MessageAction.Delete(_))
      .via(SqsAckFlow.grouped(queue, SqsAckGroupedSettings().withMaxBatchSize(5)))
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
      .map(MessageAction.Delete(_))
      .via(SqsAckFlow.grouped("queue", SqsAckGroupedSettings.Defaults))
      .runWith(Sink.ignore)
    future.failed.futureValue shouldBe a[SqsBatchException]
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
      .map(MessageAction.Delete(_))
      .via(SqsAckFlow.grouped("queue", SqsAckGroupedSettings.Defaults))
      .runWith(Sink.ignore)

    future.failed.futureValue shouldBe a[SqsBatchException]
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
      .map(MessageAction.Delete(_))
      .via(SqsAckFlow.grouped("queue", SqsAckGroupedSettings.Defaults))
      .runWith(Sink.ignore)

    future.failed.futureValue shouldBe a[RuntimeException]
  }

  it should "delete, delay & ignore all messages in batches of given size" in {
    val queue = randomQueueUrl()
    implicit val awsSqsClient = spy(sqsClient)

    val messages = for (i <- 0 until 10) yield new SendMessageRequest().withMessageBody(s"Message - $i")

    val future1 = Source(messages).via(SqsPublishFlow(queue)).runWith(Sink.ignore)
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
      .via(SqsAckFlow.grouped(queue, SqsAckGroupedSettings.Defaults))
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

  it should "delay batch of messages" taggedAs Integration in {
    val queue = randomQueueUrl()
    implicit val awsSqsClient = spy(sqsClient)

    val messages = for (i <- 0 until 10) yield new SendMessageRequest().withMessageBody(s"Message - $i")

    val future1 = Source(messages).via(SqsPublishFlow(queue)).runWith(Sink.ignore)
    future1.futureValue shouldBe Done

    val future =
      //#batch-requeue
      SqsSource(queue)
        .take(10)
        .map(MessageAction.ChangeMessageVisibility(_, 5))
        .via(SqsAckFlow.grouped(queue, SqsAckGroupedSettings.Defaults))
        .runWith(Sink.ignore)
    //#batch-requeue

    future.futureValue shouldBe Done
    verify(awsSqsClient, times(1))
      .changeMessageVisibilityBatchAsync(
        any[ChangeMessageVisibilityBatchRequest],
        any[AsyncHandler[ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityBatchResult]]
      )
  }

  it should "ignore batch of messages" in {
    val messages = for (i <- 0 until 10) yield new Message().withBody(s"Message - $i")
    implicit val awsSqsClient = sqsClient

    val stream =
      //#batch-ignore
      Source(messages)
        .take(10)
        .map(MessageAction.Ignore(_))
        .via(SqsAckFlow.grouped("queue", SqsAckGroupedSettings.Defaults))
        //#batch-ignore
        .runWith(TestSink.probe[SqsAckResult])

    for (i <- 0 until 10) {
      val result = stream.requestNext()
      result.metadata shouldBe empty
      result.message shouldBe s"Message - $i"
    }
    stream.cancel()
  }

}
