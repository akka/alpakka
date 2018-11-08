/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.stream.alpakka.sqs._
import akka.stream.alpakka.sqs.scaladsl._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import com.amazonaws.services.sqs.model._
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class SqsPublishSpec extends FlatSpec with Matchers with DefaultTestContext {

  "SqsPublishSettings" should "construct settings" in {
    //#SqsPublishSettings
    val sinkSettings =
      SqsPublishSettings()
        .withMaxInFlight(10)
    //#SqsPublishSettings
    sinkSettings.maxInFlight should be(10)
  }

  it should "require valid maxInFlight" in {
    a[IllegalArgumentException] should be thrownBy {
      SqsPublishSettings().withMaxInFlight(0)
    }
  }

  it should "accept valid parameters" in {
    SqsPublishSettings().withMaxInFlight(1)
  }

  "SqsPublishBatchSettings" should "construct settings" in {
    //#SqsPublishBatchSettings
    val batchSettings =
      SqsPublishBatchSettings()
        .withConcurrentRequests(1)
    //#SqsPublishBatchSettings
    batchSettings.concurrentRequests should be(1)
  }

  "SqsPublishGroupedSettings" should "construct settings" in {
    //#SqsPublishGroupedSettings
    val batchSettings =
      SqsPublishGroupedSettings()
        .withMaxBatchSize(10)
        .withMaxBatchWait(500.millis)
        .withConcurrentRequests(1)
    //#SqsPublishGroupedSettings
    batchSettings.concurrentRequests should be(1)
  }

  "PublishSink" should "publish and pull a message" taggedAs Integration in {
    val queue = randomQueueUrl()
    implicit val awsSqsClient = sqsClient

    val future =
      //#run-string
      Source
        .single("alpakka")
        .runWith(SqsPublishSink(queue))
    //#run-string
    Await.ready(future, 1.second)

    val probe = SqsSource(queue, SqsSourceSettings.Defaults).runWith(TestSink.probe[Message])
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
        .runWith(SqsPublishSink.messageSink(queue))
    //#run-send-request

    future.futureValue shouldBe Done
    val probe = SqsSource(queue, SqsSourceSettings.Defaults).runWith(TestSink.probe[Message])
    probe.requestNext().getBody shouldBe "alpakka"
    probe.cancel()
  }

  it should "publish messages by grouping and pull them" taggedAs Integration in {
    val queue = randomQueueUrl()
    implicit val awsSqsClient = sqsClient

    //#group
    val messages = for (i <- 0 until 20) yield s"Message - $i"

    val future = Source(messages)
      .runWith(SqsPublishSink.grouped(queue))
    //#group

    future.futureValue shouldBe Done
    val probe = SqsSource(queue, SqsSourceSettings.Defaults).runWith(TestSink.probe[Message])
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
      .runWith(SqsPublishSink.batch(queue))
    //#batch-string

    future.futureValue shouldBe Done
    val probe = SqsSource(queue, SqsSourceSettings.Defaults).runWith(TestSink.probe[Message])
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
      .runWith(SqsPublishSink.batchedMessageSink(queue))
    //#batch-send-request

    future.futureValue shouldBe Done
    val probe = SqsSource(queue, SqsSourceSettings.Defaults).runWith(TestSink.probe[Message])
    var nrOfMessages = 0
    for (i <- 0 until 10) {
      probe.requestNext()
      nrOfMessages += 1
    }

    assert(nrOfMessages == 10)
    probe.cancel()
  }

  "PublishFlow" should "put message in a flow, then pass the result further" in {
    val queue = randomQueueUrl()
    implicit val awsSqsClient = sqsClient

    val future =
      //#flow
      Source
        .single(new SendMessageRequest().withMessageBody("alpakka"))
        .via(SqsPublishFlow(queue))
        .runWith(Sink.foreach(result => println(result)))

    //#flow

    future.futureValue shouldBe Done

    SqsSource(queue, SqsSourceSettings.Defaults)
      .map(_.getBody)
      .runWith(TestSink.probe[String])
      .request(1)
      .expectNext("alpakka")
      .cancel()
  }

  "PublishFlow" should "put message in a flow, then pass the result further with dynamic queue" in {
    val queue = randomQueueUrl()
    implicit val awsSqsClient = sqsClient

    val future =
      //#flow
      Source
        .single(new SendMessageRequest().withMessageBody("alpakka").withQueueUrl(queue))
        .via(SqsPublishFlow())
        .runWith(Sink.foreach(result => println(result)))
    //#flow

    future.futureValue shouldBe Done

    SqsSource(queue, SqsSourceSettings.Defaults)
      .map(_.getBody)
      .runWith(TestSink.probe[String])
      .request(1)
      .expectNext("alpakka")
      .cancel()
  }
}
