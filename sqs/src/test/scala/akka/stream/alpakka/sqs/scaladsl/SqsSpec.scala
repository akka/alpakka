/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import java.util.concurrent.CompletableFuture

import akka.Done
import akka.stream.alpakka.sqs.{BatchException, MessageAction, SqsBatchAckFlowSettings, SqsSourceSettings}
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

class SqsSpec extends FlatSpec with Matchers with DefaultTestContext {

  private val sqsSourceSettings = SqsSourceSettings.Defaults

  it should "publish and pull a message" taggedAs Integration in {
    val queue = randomQueueUrl()
    implicit val awsSqsClient = sqsClient

    val future =
      //#run-string
      Source
        .single("alpakka")
        .runWith(SqsSink(queue))
    //#run-string
    Await.ready(future, 1.second)

    val probe = SqsSource(queue, sqsSourceSettings).runWith(TestSink.probe[Message])
    probe.requestNext().getBody shouldBe "alpakka"
    probe.cancel()
  }

  it should "publish and pull a message provided as a SendMessageRequest" taggedAs Integration in {
    val queue = randomQueueUrl()
    implicit val awsSqsClient = sqsClient

    val future =
      //#run-send-request
      Source
        .single(new SendMessageRequest().withMessageBody("alpakka"))
        .runWith(SqsSink.messageSink(queue))
    //#run-send-request

    future.futureValue shouldBe Done
    val probe = SqsSource(queue, sqsSourceSettings).runWith(TestSink.probe[Message])
    probe.requestNext().getBody shouldBe "alpakka"
    probe.cancel()
  }

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

  it should "pull and delete message" taggedAs Integration in {
    val queue = randomQueueUrl()
    sqsClient.sendMessage(queue, "alpakka-2")
    implicit val awsSqsClient = spy(sqsClient)

    val future =
      //#ack
      SqsSource(queue)
        .take(1)
        .map { m: Message =>
          (m, MessageAction.Delete)
        }
        .runWith(SqsAckSink(queue))
    //#ack

    future.futureValue shouldBe Done
    verify(awsSqsClient).deleteMessageAsync(any[DeleteMessageRequest],
                                            any[AsyncHandler[DeleteMessageRequest, DeleteMessageResult]])
  }

  it should "pull and delete message via flow" taggedAs Integration in {
    val queue = randomQueueUrl()
    sqsClient.sendMessage(queue, "alpakka-flow-ack")

    implicit val awsSqsClient = spy(sqsClient)
    val future =
      //#flow-ack
      SqsSource(queue)
        .take(1)
        .map { m: Message =>
          (m, MessageAction.Delete)
        }
        .via(SqsAckFlow(queue))
        .runWith(Sink.ignore)
    //#flow-ack

    future.futureValue shouldBe Done
    verify(awsSqsClient).deleteMessageAsync(any[DeleteMessageRequest],
                                            any[AsyncHandler[DeleteMessageRequest, DeleteMessageResult]])
  }

  it should "put message in a flow, then pass the result further" in {
    val queue = randomQueueUrl()
    implicit val awsSqsClient = sqsClient

    val future =
      //#flow
      Source
        .single(new SendMessageRequest().withMessageBody("alpakka"))
        .via(SqsFlow(queue))
        .runWith(Sink.foreach(result => println(result.message)))
    //#flow

    future.futureValue shouldBe Done

    SqsSource(queue, sqsSourceSettings)
      .map(_.getBody)
      .runWith(TestSink.probe[String])
      .request(1)
      .expectNext("alpakka")
      .cancel()
  }

  it should "pull and delay a message" taggedAs Integration in {
    val queue = randomQueueUrl()
    sqsClient.sendMessage(queue, "alpakka-3")

    implicit val awsSqsClient = spy(sqsClient)
    //#requeue
    val future = SqsSource(queue)
      .take(1)
      .map { m: Message =>
        (m, MessageAction.ChangeMessageVisibility(5))
      }
      .runWith(SqsAckSink(queue))
    //#requeue

    Await.result(future, 1.second) shouldBe Done
    verify(awsSqsClient).changeMessageVisibilityAsync(
      any[ChangeMessageVisibilityRequest],
      any[AsyncHandler[ChangeMessageVisibilityRequest, ChangeMessageVisibilityResult]]
    )
  }

  it should "publish messages by grouping and pull them" taggedAs Integration in {
    val queue = randomQueueUrl()
    implicit val awsSqsClient = sqsClient

    //#group
    val messages = for (i <- 0 until 20) yield s"Message - $i"

    val future = Source(messages)
      .runWith(SqsSink.grouped(queue))
    //#group

    future.futureValue shouldBe Done
    val probe = SqsSource(queue, sqsSourceSettings).runWith(TestSink.probe[Message])
    var nrOfMessages = 0
    for (i <- 0 until 20) {
      probe.requestNext()
      nrOfMessages += 1
    }

    assert(nrOfMessages == 20)
    probe.cancel()
  }

  it should "pull messages from a fifo queue following the same production order" taggedAs Integration in {
    val queue = randomFifoQueueUrl()
    implicit val awsSqsClient = sqsClient

    val messages = for (i <- 0 until 10) yield s"Message - $i"

    messages
      .map(msg => {
        awsSqsClient
          .sendMessage(new SendMessageRequest(queue, msg).withMessageGroupId("group1"))
      })

    val probe = SqsSource(queue, sqsSourceSettings)
      .runWith(TestSink.probe[Message])

    for (i <- 0 until 10) {
      val result = probe.requestNext()
      result.getBody shouldBe s"Message - $i"
    }

    probe.cancel()
  }

  it should "publish batch of messages and pull them" taggedAs Integration in {
    val queue = randomQueueUrl()
    implicit val awsSqsClient = sqsClient

    //#batch-string
    val messages = for (i <- 0 until 10) yield s"Message - $i"

    val future = Source
      .single(messages)
      .runWith(SqsSink.batch(queue))
    //#batch-string

    future.futureValue shouldBe Done
    val probe = SqsSource(queue, sqsSourceSettings).runWith(TestSink.probe[Message])
    var nrOfMessages = 0
    for (i <- 0 until 10) {
      probe.requestNext()
      nrOfMessages += 1
    }

    assert(nrOfMessages == 10)
    probe.cancel()
  }

  it should "publish batch of SendMessageRequests and pull them" taggedAs Integration in {
    val queue = randomQueueUrl()
    implicit val awsSqsClient = sqsClient

    //#batch-send-request
    val messages = for (i <- 0 until 10) yield new SendMessageRequest().withMessageBody(s"Message - $i")

    val future = Source
      .single(messages)
      .runWith(SqsSink.batchedMessageSink(queue))
    //#batch-send-request

    future.futureValue shouldBe Done
    val probe = SqsSource(queue, sqsSourceSettings).runWith(TestSink.probe[Message])
    var nrOfMessages = 0
    for (i <- 0 until 10) {
      probe.requestNext()
      nrOfMessages += 1
    }

    assert(nrOfMessages == 10)
    probe.cancel()
  }

  it should "pull and ignore a message" taggedAs Integration in {
    val queue = randomQueueUrl()
    sqsClient.sendMessage(queue, "alpakka-4")

    implicit val awsSqsClient = spy(sqsClient)
    val result =
      //#ignore
      SqsSource(queue)
        .take(1)
        .map { m: Message =>
          (m, MessageAction.Ignore)
        }
        .via(SqsAckFlow(queue))
        //#ignore
        .runWith(TestSink.probe[AckResult])
        .requestNext(1.second)

    result.metadata shouldBe empty
    result.message shouldBe "alpakka-4"
  }

  it should "delete batch of messages" taggedAs Integration in {
    val queue = randomQueueUrl()
    implicit val awsSqsClient = spy(sqsClient)

    val messages = for (i <- 0 until 10) yield new SendMessageRequest().withMessageBody(s"Message - $i")

    val future1 = Source(messages).via(SqsFlow(queue)).runWith(Sink.ignore)
    future1.futureValue shouldBe Done

    val future =
      //#batch-ack
      SqsSource(queue)
        .take(10)
        .map { m: Message =>
          (m, MessageAction.Delete)
        }
        .via(SqsAckFlow.grouped(queue, SqsBatchAckFlowSettings.Defaults))
        .runWith(Sink.ignore)
    //#batch-ack

    future.futureValue shouldBe Done
    verify(awsSqsClient, times(1)).deleteMessageBatchAsync(
      any[DeleteMessageBatchRequest],
      any[AsyncHandler[DeleteMessageBatchRequest, DeleteMessageBatchResult]]
    )
  }

  it should "delay batch of messages" taggedAs Integration in {
    val queue = randomQueueUrl()
    implicit val awsSqsClient = spy(sqsClient)

    val messages = for (i <- 0 until 10) yield new SendMessageRequest().withMessageBody(s"Message - $i")

    val future1 = Source(messages).via(SqsFlow(queue)).runWith(Sink.ignore)
    future1.futureValue shouldBe Done

    val future =
      //#batch-requeue
      SqsSource(queue)
        .take(10)
        .map { m: Message =>
          (m, MessageAction.ChangeMessageVisibility(5))
        }
        .via(SqsAckFlow.grouped(queue, SqsBatchAckFlowSettings.Defaults))
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
        .map { m: Message =>
          (m, MessageAction.Ignore)
        }
        .via(SqsAckFlow.grouped("queue", SqsBatchAckFlowSettings.Defaults))
        //#batch-ignore
        .runWith(TestSink.probe[AckResult])

    for (i <- 0 until 10) {
      val result = stream.requestNext()
      result.metadata shouldBe empty
      result.message shouldBe s"Message - $i"
    }
    stream.cancel()
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
        (m, MessageAction.Delete)
      }
      .via(SqsAckFlow.grouped(queue, SqsBatchAckFlowSettings.Defaults.copy(maxBatchSize = 5)))
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
        (m, MessageAction.Delete)
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
        (m, MessageAction.Delete)
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
        (m, MessageAction.Delete)
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
            (m, MessageAction.Delete)
          else if (i % 3 == 1)
            (m, MessageAction.ChangeMessageVisibility(5))
          else
            (m, MessageAction.Ignore)
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
