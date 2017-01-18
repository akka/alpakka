/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.scaladsl

import akka.Done
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import com.amazonaws.services.sqs.model.Message
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class SqsSpec extends FlatSpec with Matchers with DefaultTestContext {

  it should "publish and pull a message" taggedAs Integration in {
    val queue = randomQueueUrl()

    //#run
    val future = Source.single("alpakka").runWith(SqsSink(queue))
    Await.ready(future, 1.second)
    //#run

    val probe = SqsSource(queue).runWith(TestSink.probe[Message])
    probe.requestNext().getBody shouldBe "alpakka"
    probe.cancel()
  }

  it should "pull a message and publish it to another queue" taggedAs Integration in {
    val queue1 = randomQueueUrl()
    val queue2 = randomQueueUrl()

    sqsClient.sendMessage(queue1, "alpakka")

    val future = SqsSource(queue1)
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
}
