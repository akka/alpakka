/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.stream.alpakka.sqs._
import akka.stream.alpakka.sqs.scaladsl._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.model._
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{spy, times, verify}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

class SqsFlowSnippetsSpec extends FlatSpec with Matchers with DefaultTestContext {

  it should "put message in a flow, then pass the result further" in {
    val queue = randomQueueUrl()
    implicit val awsSqsClient = sqsClient

    val future =
      //#flow
      Source
        .single(new SendMessageRequest().withMessageBody("alpakka"))
        .via(SqsPublishFlow(queue))
        .runWith(Sink.foreach(result => println(result.message)))
    //#flow

    future.futureValue shouldBe Done

    SqsSource(queue, SqsSourceSettings.Defaults)
      .map(_.getBody)
      .runWith(TestSink.probe[String])
      .request(1)
      .expectNext("alpakka")
      .cancel()
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
          MessageAction.Delete(m)
        }
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
        .map { m: Message =>
          MessageAction.Ignore(m)
        }
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
        .map { m: Message =>
          MessageAction.Delete(m)
        }
        .via(SqsAckFlow.grouped(queue, SqsAckGroupedSettings.Defaults))
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

    val future1 = Source(messages).via(SqsPublishFlow(queue)).runWith(Sink.ignore)
    future1.futureValue shouldBe Done

    val future =
      //#batch-requeue
      SqsSource(queue)
        .take(10)
        .map { m: Message =>
          MessageAction.ChangeMessageVisibility(m, 5)
        }
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
        .map { m: Message =>
          MessageAction.Ignore(m)
        }
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
