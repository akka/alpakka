/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.util.concurrent.CompletableFuture

import akka.Done
import akka.stream.alpakka.sqs._
import akka.stream.alpakka.sqs.scaladsl._
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

import scala.concurrent.duration._
import scala.collection.JavaConverters._

class SqsAckSpec extends FlatSpec with Matchers with DefaultTestContext {

  trait IntegrationFixture {
    val queueUrl: String = randomQueueUrl()
    implicit val awsSqsClient: AmazonSQSAsync = spy(sqsClient)

    def sendMessage(message: String): Unit =
      awsSqsClient.sendMessage(queueUrl, message)

    def sendMessages(messages: Seq[String]): Unit = {
      val batch = messages.zipWithIndex.map {
        case (m, i) => new SendMessageBatchRequestEntry().withMessageBody(m).withId(i.toString)
      }
      awsSqsClient.sendMessageBatch(queueUrl, batch.asJava)
    }
  }

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

  "AckSink" should "pull and delete message" taggedAs Integration in new IntegrationFixture {
    sendMessage("alpakka-2")

    val future =
      //#ack
      SqsSource(queueUrl)
        .take(1)
        .map(MessageAction.Delete(_))
        .runWith(SqsAckSink(queueUrl))
    //#ack

    future.futureValue shouldBe Done
    verify(awsSqsClient).deleteMessageAsync(
      any[DeleteMessageRequest],
      any[AsyncHandler[DeleteMessageRequest, DeleteMessageResult]]
    )
  }

  it should "pull and delay a message" taggedAs Integration in new IntegrationFixture {
    sendMessage("alpakka-3")

    val future =
      //#requeue
      SqsSource(queueUrl)
        .take(1)
        .map(MessageAction.ChangeMessageVisibility(_, 5))
        .runWith(SqsAckSink(queueUrl))
    //#requeue

    future.futureValue shouldBe Done
    verify(awsSqsClient).changeMessageVisibilityAsync(
      any[ChangeMessageVisibilityRequest],
      any[AsyncHandler[ChangeMessageVisibilityRequest, ChangeMessageVisibilityResult]]
    )
  }

  it should "pull and ignore a message" taggedAs Integration in new IntegrationFixture {
    sendMessage("alpakka-flow-ack")

    //#ignore
    SqsSource(queueUrl)
      .map(MessageAction.Ignore(_))
      .runWith(SqsAckSink(queueUrl))
    //#ignore

    // TODO: assertions missing
  }

  "AckFlow" should "pull and delete message via flow" taggedAs Integration in new IntegrationFixture {
    sendMessage("alpakka-flow-ack")

    val future =
      //#flow-ack
      SqsSource(queueUrl)
        .take(1)
        .map(MessageAction.Delete(_))
        .via(SqsAckFlow(queueUrl))
        .runWith(Sink.head)
    //#flow-ack

    val result = future.futureValue
    result.metadata shouldBe defined
    result.messageAction shouldBe a[MessageAction.Delete]
    result.messageAction.message.getBody shouldBe "alpakka-flow-ack"
    verify(awsSqsClient).deleteMessageAsync(any[DeleteMessageRequest],
                                            any[AsyncHandler[DeleteMessageRequest, DeleteMessageResult]])
  }

  it should "pull and ignore a message" taggedAs Integration in new IntegrationFixture {
    sendMessage("alpakka-4")

    val future =
      SqsSource(queueUrl)
        .take(1)
        .map(MessageAction.Ignore(_))
        .via(SqsAckFlow(queueUrl))
        .runWith(Sink.head)

    val result = future.futureValue
    result.metadata shouldBe empty
    result.messageAction shouldBe a[MessageAction.Ignore]
    result.messageAction.message.getBody shouldBe "alpakka-4"
  }

  it should "delete batch of messages" taggedAs Integration in new IntegrationFixture {
    val messages = for (i <- 0 until 10) yield s"Message - $i"
    sendMessages(messages)

    val future =
      //#batch-ack
      SqsSource(queueUrl)
        .take(10)
        .map(MessageAction.Delete(_))
        .via(SqsAckFlow.grouped(queueUrl, SqsAckGroupedSettings.Defaults))
        .runWith(Sink.seq)
    //#batch-ack

    val results = future.futureValue
    results.size shouldBe 10
    results.foreach { r =>
      r.metadata shouldBe defined
      r.messageAction shouldBe a[MessageAction.Delete]
    }
    results.map(_.messageAction.message.getBody) should contain theSameElementsAs messages
    verify(awsSqsClient, times(1)).deleteMessageBatchAsync(
      any[DeleteMessageBatchRequest],
      any[AsyncHandler[DeleteMessageBatchRequest, DeleteMessageBatchResult]]
    )
  }

  it should "delete all messages in batches of given size" taggedAs Integration in new IntegrationFixture {
    val messages = for (i <- 0 until 10) yield s"Message - $i"
    sendMessages(messages)

    val future = SqsSource(queueUrl)
      .take(10)
      .map(MessageAction.Delete(_))
      .via(SqsAckFlow.grouped(queueUrl, SqsAckGroupedSettings().withMaxBatchSize(5)))
      .runWith(Sink.seq)

    val results = future.futureValue
    results.foreach { r =>
      r.metadata shouldBe defined
      r.messageAction shouldBe a[MessageAction.Delete]
    }
    results.map(_.messageAction.message.getBody) should contain theSameElementsAs messages
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

  it should "delete, delay & ignore all messages in batches of given size" taggedAs Integration in new IntegrationFixture {
    val messages = for (i <- 0 until 10) yield s"Message - $i"
    sendMessages(messages)

    val future = SqsSource(queueUrl)
      .take(10)
      .zipWithIndex
      .map {
        case (m, i) if i % 3 == 0 => MessageAction.Delete(m)
        case (m, i) if i % 3 == 1 => MessageAction.ChangeMessageVisibility(m, 5)
        case (m, _) => MessageAction.Ignore(m)
      }
      .via(SqsAckFlow.grouped(queueUrl, SqsAckGroupedSettings.Defaults))
      .runWith(Sink.seq)

    val results = future.futureValue
    results.count(_.messageAction.isInstanceOf[MessageAction.Delete]) shouldBe 4
    results.count(_.messageAction.isInstanceOf[MessageAction.ChangeMessageVisibility]) shouldBe 3
    results.count(_.messageAction.isInstanceOf[MessageAction.Ignore]) shouldBe 3
    results.map(_.messageAction.message.getBody) should contain theSameElementsAs messages

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

  it should "delay batch of messages" taggedAs Integration in new IntegrationFixture {
    val messages = for (i <- 0 until 10) yield s"Message - $i"
    sendMessages(messages)

    val future =
      //#batch-requeue
      SqsSource(queueUrl)
        .take(10)
        .map(MessageAction.ChangeMessageVisibility(_, 5))
        .via(SqsAckFlow.grouped(queueUrl, SqsAckGroupedSettings.Defaults))
        .runWith(Sink.seq)
    //#batch-requeue

    val results = future.futureValue
    results.foreach { r =>
      r.metadata shouldBe defined
      r.messageAction shouldBe a[MessageAction.ChangeMessageVisibility]
    }
    results.map(_.messageAction.message.getBody) should contain theSameElementsAs messages
    verify(awsSqsClient, times(1))
      .changeMessageVisibilityBatchAsync(
        any[ChangeMessageVisibilityBatchRequest],
        any[AsyncHandler[ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityBatchResult]]
      )
  }

  it should "ignore batch of messages" in {
    val messages = for (i <- 0 until 10) yield new Message().withBody(s"Message - $i")
    implicit val mockAwsSqsClient = mock[AmazonSQSAsync]

    val future =
      //#batch-ignore
      Source(messages)
        .take(10)
        .map(MessageAction.Ignore(_))
        .via(SqsAckFlow.grouped("queue", SqsAckGroupedSettings.Defaults))
        .runWith(Sink.seq)
    //#batch-ignore

    val results = future.futureValue
    results.foreach { r =>
      r.metadata shouldBe empty
      r.messageAction shouldBe a[MessageAction.Ignore]
    }
    results.map(_.messageAction.message) should contain theSameElementsAs messages
  }

}
