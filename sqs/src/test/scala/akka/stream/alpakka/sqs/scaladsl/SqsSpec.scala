/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.scaladsl

import akka.Done
import akka.stream.alpakka.sqs.{Ack, RequeueWithDelay, SqsSourceSettings}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import com.amazonaws.services.sqs.model.{DeleteMessageRequest, Message, SendMessageRequest}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{spy, verify}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class SqsSpec extends FlatSpec with Matchers with DefaultTestContext {

  private val sqsSourceSettings = SqsSourceSettings.Defaults

  it should "publish and pull a message" taggedAs Integration in {
    val queue = randomQueueUrl()

    //#run
    val future = Source.single("alpakka").runWith(SqsSink(queue))
    Await.ready(future, 1.second)
    //#run

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
        (m, Ack())
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
        (m, Ack())
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
    val future = Source.single("alpakka").via(SqsFlow(queue)).runWith(Sink.ignore)
    //#flow

    Await.result(future, 1.second) shouldBe Done

    SqsSource(queue, sqsSourceSettings)
      .map(_.getBody)
      .runWith(TestSink.probe[String])
      .request(1)
      .expectNext("alpakka")
      .cancel()
  }

  it should "pull and requeue message" taggedAs Integration in {
    val queue = randomQueueUrl()
    sqsClient.sendMessage(queue, "alpakka-3")

    val awsSqsClient = spy(sqsClient)
    //#requeue
    val future = SqsSource(queue)(awsSqsClient)
      .take(1)
      .map { m: Message =>
        (m, RequeueWithDelay(5))
      }
      .runWith(SqsAckSink(queue)(awsSqsClient))
    //#requeue

    Await.result(future, 1.second) shouldBe Done
    verify(awsSqsClient).sendMessageAsync(any[SendMessageRequest], any)
  }

}
