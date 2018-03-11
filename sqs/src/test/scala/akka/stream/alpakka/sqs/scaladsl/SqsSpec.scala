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

    //#run-string
    val future = Source.single("alpakka").runWith(SqsSink(queue))
    Await.ready(future, 1.second)
    //#run-string

    val probe = SqsSource(queue, sqsSourceSettings).runWith(TestSink.probe[Message])
    probe.requestNext().getBody shouldBe "alpakka"
    probe.cancel()
  }

  it should "publish and pull a message provided as a SendMessageRequest" taggedAs Integration in {
    val queue = randomQueueUrl()

    //#run-send-request
    val future = Source.single(new SendMessageRequest().withMessageBody("alpakka")).runWith(SqsSink.messageSink(queue))
    Await.ready(future, 1.second)
    //#run-send-request

    val probe = SqsSource(queue, sqsSourceSettings).runWith(TestSink.probe[Message])
    probe.requestNext().getBody shouldBe "alpakka"
    probe.cancel()
  }

  it should "pull a message and publish it to another queue" taggedAs Integration in {
    val queue1 = randomQueueUrl()
    val queue2 = randomQueueUrl()

    sqsClient.sendMessage(queue1, "alpakka")

    val future = SqsSource(queue1, sqsSourceSettings)
      .take(1)
      .map { m: Message =>
        m.getBody
      }
      .runWith(SqsSink(queue2))
    Await.result(future, 1.second) shouldBe Done

    val result = sqsClient.receiveMessage(queue2)
    result.getMessages.size() shouldBe 1
    result.getMessages.get(0).getBody shouldBe "alpakka"
  }

  it should "pull and delete message" taggedAs Integration in {
    val queue = randomQueueUrl()
    sqsClient.sendMessage(queue, "alpakka-2")

    val awsSqsClient = spy(sqsClient)
    //#ack
    val future = SqsSource(queue)(awsSqsClient)
      .take(1)
      .map { m: Message =>
        (m, MessageAction.Delete)
      }
      .runWith(SqsAckSink(queue)(awsSqsClient))
    //#ack

    Await.result(future, 1.second) shouldBe Done
    verify(awsSqsClient).deleteMessageAsync(any[DeleteMessageRequest], any)
  }

  it should "pull and delete message via flow" taggedAs Integration in {
    val queue = randomQueueUrl()
    sqsClient.sendMessage(queue, "alpakka-flow-ack")

    val awsSqsClient = spy(sqsClient)
    //#flow-ack
    val future = SqsSource(queue)(awsSqsClient)
      .take(1)
      .map { m: Message =>
        (m, MessageAction.Delete)
      }
      .via(SqsAckFlow(queue)(awsSqsClient))
      .runWith(Sink.ignore)
    //#flow-ack

    Await.result(future, 1.second) shouldBe Done
    verify(awsSqsClient).deleteMessageAsync(any[DeleteMessageRequest], any)
  }

  it should "put message in a flow, then pass the result further" in {
    val queue = randomQueueUrl()

    //#flow
    val future = Source.single(new SendMessageRequest(queue, "alpakka")).via(SqsFlow(queue)).runWith(Sink.ignore)
    //#flow

    Await.result(future, 1.second) shouldBe Done

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

    val awsSqsClient = spy(sqsClient)
    //#requeue
    val future = SqsSource(queue)(awsSqsClient)
      .take(1)
      .map { m: Message =>
        (m, MessageAction.ChangeMessageVisibility(5))
      }
      .runWith(SqsAckSink(queue)(awsSqsClient))
    //#requeue

    Await.result(future, 1.second) shouldBe Done
    verify(awsSqsClient).changeMessageVisibilityAsync(
      any[ChangeMessageVisibilityRequest],
      any[AsyncHandler[ChangeMessageVisibilityRequest, ChangeMessageVisibilityResult]]
    )
  }

  it should "publish messages by grouping and pull them" taggedAs Integration in {
    val queue = randomQueueUrl()

    //#group
    val messages = for (i <- 0 until 20) yield s"Message - $i"

    val future = Source(messages).runWith(SqsSink.grouped(queue))
    Await.ready(future, 1.second)
    //#group

    val probe = SqsSource(queue, sqsSourceSettings).runWith(TestSink.probe[Message])
    var nrOfMessages = 0
    for (i <- 0 until 20) {
      probe.requestNext()
      nrOfMessages += 1
    }

    assert(nrOfMessages == 20)
    probe.cancel()
  }

  it should "publish batch of messages and pull them" taggedAs Integration in {
    val queue = randomQueueUrl()

    //#batch-string
    val messages = for (i <- 0 until 10) yield s"Message - $i"

    val future = Source.single(messages).runWith(SqsSink.batch(queue))
    Await.ready(future, 1.second)
    //#batch-string

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

    //#batch-send-request
    val messages = for (i <- 0 until 10) yield new SendMessageRequest().withMessageBody(s"Message - $i")

    val future = Source.single(messages).runWith(SqsSink.batchedMessageSink(queue))
    Await.ready(future, 1.second)
    //#batch-send-request

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

    val awsSqsClient = spy(sqsClient)
    //#ignore
    val result = SqsSource(queue)(awsSqsClient)
      .take(1)
      .map { m: Message =>
        (m, MessageAction.Ignore)
      }
      .via(SqsAckFlow(queue)(awsSqsClient))
      .runWith(TestSink.probe[AckResult])
      .requestNext(1.second)
    //#ignore

    result.metadata shouldBe empty
    result.message shouldBe "alpakka-4"
  }

  it should "delete batch of messages" taggedAs Integration in {
    val queue = randomQueueUrl()

    val messages = for (i <- 0 until 10) yield new SendMessageRequest().withMessageBody(s"Message - $i")

    var future = Source(messages).via(SqsFlow(queue)).runWith(Sink.ignore)

    Await.result(future, 1.second) shouldBe Done

    val awsSqsClient = spy(sqsClient)
    //#batch-ack
    future = SqsSource(queue)(awsSqsClient)
      .take(10)
      .map { m: Message =>
        (m, MessageAction.Delete)
      }
      .via(SqsAckFlow.grouped(queue, SqsBatchAckFlowSettings.Defaults)(awsSqsClient))
      .runWith(Sink.ignore)
    //#batch-ack

    Await.result(future, 1.second) shouldBe Done
    verify(awsSqsClient, times(1)).deleteMessageBatchAsync(any[DeleteMessageBatchRequest], any)
  }

  it should "delay batch of messages" taggedAs Integration in {
    val queue = randomQueueUrl()

    val messages = for (i <- 0 until 10) yield new SendMessageRequest().withMessageBody(s"Message - $i")

    var future = Source(messages).via(SqsFlow(queue)).runWith(Sink.ignore)

    Await.result(future, 1.second) shouldBe Done

    val awsSqsClient = spy(sqsClient)
    //#batch-requeue
    future = SqsSource(queue)(awsSqsClient)
      .take(10)
      .map { m: Message =>
        (m, MessageAction.ChangeMessageVisibility(5))
      }
      .via(SqsAckFlow.grouped(queue, SqsBatchAckFlowSettings.Defaults)(awsSqsClient))
      .runWith(Sink.ignore)
    //#batch-requeue

    Await.result(future, 1.second) shouldBe Done
    verify(awsSqsClient, times(1))
      .changeMessageVisibilityBatchAsync(any[ChangeMessageVisibilityBatchRequest], any)
  }

  it should "ignore batch of messages" in {
    val messages = for (i <- 0 until 10) yield new Message().withBody(s"Message - $i")

    //#batch-ignore
    val stream = Source(messages)
      .take(10)
      .map { m: Message =>
        (m, MessageAction.Ignore)
      }
      .via(SqsAckFlow.grouped("queue", SqsBatchAckFlowSettings.Defaults))
      .runWith(TestSink.probe[AckResult])
    //#batch-ignore

    for (i <- 0 until 10) {
      val result = stream.requestNext()
      result.metadata shouldBe empty
      result.message shouldBe s"Message - $i"
    }
    stream.cancel()
  }

  it should "delete all messages in batches of given size" taggedAs Integration in {
    val queue = randomQueueUrl()

    val messages = for (i <- 0 until 10) yield new SendMessageRequest().withMessageBody(s"Message - $i")

    var future = Source(messages).via(SqsFlow(queue)).runWith(Sink.ignore)

    Await.result(future, 1.second) shouldBe Done

    val awsSqsClient = spy(sqsClient)
    future = SqsSource(queue)(awsSqsClient)
      .take(10)
      .map { m: Message =>
        (m, MessageAction.Delete)
      }
      .via(SqsAckFlow.grouped(queue, SqsBatchAckFlowSettings.Defaults.copy(maxBatchSize = 5))(awsSqsClient))
      .runWith(Sink.ignore)

    Await.result(future, 1.second) shouldBe Done
    verify(awsSqsClient, times(2)).deleteMessageBatchAsync(any[DeleteMessageBatchRequest], any)
  }

  it should "fail if any of the messages in the batch request failed" in {
    val messages = for (i <- 0 until 10) yield new Message().withBody(s"Message - $i")

    val mockAwsSqsClient = mock[AmazonSQSAsync]
    when(mockAwsSqsClient.deleteMessageBatchAsync(any[DeleteMessageBatchRequest], any))
      .thenAnswer(new Answer[AnyRef] {
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
      .via(SqsAckFlow.grouped("queue", SqsBatchAckFlowSettings.Defaults)(mockAwsSqsClient))
      .runWith(Sink.ignore)

    a[BatchException] should be thrownBy Await.result(future, 1.second)
  }

  it should "fail if the batch request failed" in {
    val messages = for (i <- 0 until 10) yield new Message().withBody(s"Message - $i")

    val mockAwsSqsClient = mock[AmazonSQSAsync]
    when(mockAwsSqsClient.deleteMessageBatchAsync(any[DeleteMessageBatchRequest], any))
      .thenAnswer(new Answer[AnyRef] {
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
      .via(SqsAckFlow.grouped("queue", SqsBatchAckFlowSettings.Defaults)(mockAwsSqsClient))
      .runWith(Sink.ignore)

    a[BatchException] should be thrownBy Await.result(future, 1.second)
  }

  it should "fail if the client invocation failed" in {
    val messages = for (i <- 0 until 10) yield new Message().withBody(s"Message - $i")

    val mockAwsSqsClient = mock[AmazonSQSAsync]
    when(mockAwsSqsClient.deleteMessageBatchAsync(any[DeleteMessageBatchRequest], any))
      .thenThrow(new RuntimeException("error"))

    val future = Source(messages)
      .take(10)
      .map { m: Message =>
        (m, MessageAction.Delete)
      }
      .via(SqsAckFlow.grouped("queue", SqsBatchAckFlowSettings.Defaults)(mockAwsSqsClient))
      .runWith(Sink.ignore)

    a[RuntimeException] should be thrownBy Await.result(future, 1.second)
  }

  it should "delete, delay & ignore all messages in batches of given size" in {
    val queue = randomQueueUrl()

    val messages = for (i <- 0 until 10) yield new SendMessageRequest().withMessageBody(s"Message - $i")

    var future = Source(messages).via(SqsFlow(queue)).runWith(Sink.ignore)

    Await.result(future, 1.second) shouldBe Done

    val awsSqsClient = spy(sqsClient)
    var i = 0
    future = SqsSource(queue)(awsSqsClient)
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
      .via(SqsAckFlow.grouped(queue, SqsBatchAckFlowSettings.Defaults)(awsSqsClient))
      .runWith(Sink.ignore)

    Await.result(future, 1.second) shouldBe Done
    verify(awsSqsClient, times(1)).deleteMessageBatchAsync(any[DeleteMessageBatchRequest], any)
    verify(awsSqsClient, times(1))
      .changeMessageVisibilityBatchAsync(any[ChangeMessageVisibilityBatchRequest], any)
  }
}
