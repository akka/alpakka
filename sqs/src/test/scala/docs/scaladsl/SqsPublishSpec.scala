/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.stream.alpakka.sqs._
import akka.stream.alpakka.sqs.scaladsl._
import akka.stream.scaladsl.{Sink, Source}
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model._
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class SqsPublishSpec extends FlatSpec with Matchers with DefaultTestContext {

  abstract class IntegrationFixture(fifo: Boolean = false) {
    val queue: String = if (fifo) randomFifoQueueUrl() else randomQueueUrl()
    implicit val awsSqsClient: AmazonSQSAsync = sqsClient

    def receiveMessage(): Message =
      awsSqsClient.receiveMessage(queue).getMessages.asScala.head

    def receiveMessages(numberOfMessages: Int): Seq[Message] = {
      val request = new ReceiveMessageRequest()
        .withQueueUrl(queue)
        .withMaxNumberOfMessages(numberOfMessages)
      awsSqsClient.receiveMessage(request).getMessages.asScala
    }
  }

  "SqsPublishSettings" should "construct settings" in {
    //#SqsPublishSettings
    val sinkSettings =
      SqsPublishSettings()
        .withMaxInFlight(10)
    //#SqsPublishSettings
    sinkSettings.maxInFlight shouldBe 10
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
    batchSettings.concurrentRequests shouldBe 1
  }

  "SqsPublishGroupedSettings" should "construct settings" in {
    //#SqsPublishGroupedSettings
    val batchSettings =
      SqsPublishGroupedSettings()
        .withMaxBatchSize(10)
        .withMaxBatchWait(500.millis)
        .withConcurrentRequests(1)
    //#SqsPublishGroupedSettings
    batchSettings.concurrentRequests shouldBe 1
  }

  "PublishSink" should "publish and pull a message" taggedAs Integration in new IntegrationFixture {
    val future =
      //#run-string
      Source
        .single("alpakka")
        .runWith(SqsPublishSink(queue))
    //#run-string
    future.futureValue shouldBe Done

    receiveMessage().getBody shouldBe "alpakka"
  }

  it should "publish and pull a message provided as a SendMessageRequest" taggedAs Integration in new IntegrationFixture {
    val future =
      //#run-send-request
      Source
        .single(new SendMessageRequest().withMessageBody("alpakka"))
        .runWith(SqsPublishSink.messageSink(queue))
    //#run-send-request

    future.futureValue shouldBe Done

    receiveMessage().getBody shouldBe "alpakka"
  }

  it should "publish messages by grouping and pull them" taggedAs Integration in new IntegrationFixture {
    //#group
    val messages = for (i <- 0 until 20) yield s"Message - $i"

    val future = Source(messages)
      .runWith(SqsPublishSink.grouped(queue))
    //#group

    future.futureValue shouldBe Done

    receiveMessages(20) should have size 20
  }

  it should "publish batch of messages and pull them" taggedAs Integration in new IntegrationFixture {
    //#batch-string
    val messages = for (i <- 0 until 10) yield s"Message - $i"

    val future = Source
      .single(messages)
      .runWith(SqsPublishSink.batch(queue))
    //#batch-string

    future.futureValue shouldBe Done

    receiveMessages(10) should have size 10
  }

  it should "publish batch of SendMessageRequests and pull them" taggedAs Integration in new IntegrationFixture {
    //#batch-send-request
    val messages = for (i <- 0 until 10) yield new SendMessageRequest().withMessageBody(s"Message - $i")

    val future = Source
      .single(messages)
      .runWith(SqsPublishSink.batchedMessageSink(queue))
    //#batch-send-request

    future.futureValue shouldBe Done

    receiveMessages(10) should have size 10
  }

  "PublishFlow" should "put message in a flow, then pass the result further" taggedAs Integration in new IntegrationFixture {
    val future =
      //#flow
      Source
        .single(new SendMessageRequest().withMessageBody("alpakka"))
        .via(SqsPublishFlow(queue))
        .runWith(Sink.head)
    //#flow

    val result = future.futureValue
    result.message.getBody shouldBe "alpakka"
    result.fifoMessageIdentifiers shouldBe empty

    receiveMessage().getBody shouldBe "alpakka"
  }

  it should "put message in a flow, then pass the result further with dynamic queue" taggedAs Integration in new IntegrationFixture {
    val future =
      //#flow
      Source
        .single(new SendMessageRequest().withMessageBody("alpakka").withQueueUrl(queue))
        .via(SqsPublishFlow())
        .runWith(Sink.head)
    //#flow

    val result = future.futureValue
    result.message.getBody shouldBe "alpakka"
    result.fifoMessageIdentifiers shouldBe empty

    receiveMessage().getBody shouldBe "alpakka"
  }

  ignore should "put message in a flow, then pass the result further with fifo queues" taggedAs Integration in new IntegrationFixture(
    fifo = true
  ) {
    // elasticmq does not provide proper fifo support (see https://github.com/adamw/elasticmq/issues/125)
    // set your fifo sqs queue url and awsSqsClient manually
    // override val queue = "https://sqs.us-east-1.amazonaws.com/$AWS_ACCOUNT_ID/$queue_name.fifo"
    // override implicit val awsSqsClient: AmazonSQSAsync = AmazonSQSAsyncClientBuilder.standard().build()

    val future =
    //#flow
      Source
        .single(
          new SendMessageRequest()
            .withMessageBody("alpakka")
            .withMessageGroupId("group-id")
            .withMessageDeduplicationId(s"deduplication-id")
        )
        .via(SqsPublishFlow(queue))
        .runWith(Sink.head)
    //#flow

    val result = future.futureValue
    result.message.getBody shouldBe "alpakka"
    result.fifoMessageIdentifiers.map(_.sequenceNumber) shouldBe defined
    result.fifoMessageIdentifiers.map(_.messageGroupId) shouldBe Some("group-id")
    result.fifoMessageIdentifiers.flatMap(_.messageDeduplicationId) shouldBe Some("deduplication-id")

    receiveMessage().getBody shouldBe "alpakka"
  }
}
