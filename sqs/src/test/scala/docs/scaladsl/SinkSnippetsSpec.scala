/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.stream.alpakka.sqs._
import akka.stream.alpakka.sqs.scaladsl._
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.model._
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{spy, verify}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class SinkSnippetsSpec extends FlatSpec with Matchers with DefaultTestContext {

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

  it should "pull and delete message" taggedAs Integration in {
    val queue = randomQueueUrl()
    sqsClient.sendMessage(queue, "alpakka-2")
    implicit val awsSqsClient = spy(sqsClient)

    val future =
      //#ack
      SqsSource(queue)
        .take(1)
        .map { m: Message =>
          MessageAction.Delete(m)
        }
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
      .map { m: Message =>
        MessageAction.ChangeMessageVisibility(m, 5)
      }
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
      .map { m: Message =>
        MessageAction.Ignore(m)
      }
      .runWith(SqsAckSink(queue))
    //#ignore

    // TODO: assertions missing
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

}
