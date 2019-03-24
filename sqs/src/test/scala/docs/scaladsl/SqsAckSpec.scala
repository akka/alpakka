/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.function.Supplier

import akka.Done
import akka.stream.alpakka.sqs.{MessageAction, _}
import akka.stream.alpakka.sqs.scaladsl._
import akka.stream.scaladsl.{Sink, Source}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{spy, times, verify, when}
import org.scalatestplus.mockito.MockitoSugar.mock
import org.scalatest.{FlatSpec, Matchers}
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._

import scala.concurrent.duration._
import scala.collection.JavaConverters._

class SqsAckSpec extends FlatSpec with Matchers with DefaultTestContext {

  trait IntegrationFixture {
    val queueUrl: String = randomQueueUrl()
    implicit val awsSqsClient: SqsAsyncClient = spy(sqsClient)

    def sendMessage(message: String): Unit = {
      def request =
        SendMessageRequest
          .builder()
          .queueUrl(queueUrl)
          .messageBody(message)
          .build()

      awsSqsClient.sendMessage(request).get(2, TimeUnit.SECONDS)
    }

    def sendMessages(messages: Seq[String]): Unit = {
      def entries = messages.zipWithIndex.foldLeft(List.empty[SendMessageBatchRequestEntry]) {
        case (list, (m, i)) =>
          SendMessageBatchRequestEntry
            .builder()
            .messageBody(m)
            .id(i.toString)
            .build() :: list
      }

      def batch =
        SendMessageBatchRequest
          .builder()
          .queueUrl(queueUrl)
          .entries(entries.asJava)
          .build()

      awsSqsClient.sendMessageBatch(batch).get(2, TimeUnit.SECONDS)
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
      SqsSource(queueUrl, sqsSourceSettings)
        .take(1)
        .map(MessageAction.Delete(_))
        .runWith(SqsAckSink(queueUrl))
    //#ack

    future.futureValue shouldBe Done
    verify(awsSqsClient).deleteMessage(
      any[DeleteMessageRequest]
    )
  }

  it should "pull and delay a message" taggedAs Integration in new IntegrationFixture {
    sendMessage("alpakka-3")

    val future =
      //#requeue
      SqsSource(queueUrl, sqsSourceSettings)
        .take(1)
        .map(MessageAction.ChangeMessageVisibility(_, 5))
        .runWith(SqsAckSink(queueUrl))
    //#requeue

    future.futureValue shouldBe Done
    verify(awsSqsClient).changeMessageVisibility(
      any[ChangeMessageVisibilityRequest]
    )
  }

  it should "pull and ignore a message" taggedAs Integration in new IntegrationFixture {
    sendMessage("alpakka-flow-ack")

    //#ignore
    SqsSource(queueUrl, sqsSourceSettings)
      .map(MessageAction.Ignore(_))
      .runWith(SqsAckSink(queueUrl))
    //#ignore

    // TODO: assertions missing
  }

  "AckFlow" should "pull and delete message via flow" taggedAs Integration in new IntegrationFixture {
    sendMessage("alpakka-flow-ack")

    val future =
      //#flow-ack
      SqsSource(queueUrl, sqsSourceSettings)
        .take(1)
        .map(MessageAction.Delete(_))
        .via(SqsAckFlow(queueUrl))
        .runWith(Sink.head)
    //#flow-ack

    val result = future.futureValue
    result.metadata shouldBe defined
    result.messageAction shouldBe a[MessageAction.Delete]
    result.messageAction.message.body() shouldBe "alpakka-flow-ack"
    verify(awsSqsClient).deleteMessage(any[DeleteMessageRequest])
  }

  it should "pull and ignore a message" taggedAs Integration in new IntegrationFixture {
    sendMessage("alpakka-4")

    val future =
      SqsSource(queueUrl, sqsSourceSettings)
        .take(1)
        .map(MessageAction.Ignore(_))
        .via(SqsAckFlow(queueUrl))
        .runWith(Sink.head)

    val result = future.futureValue
    result.metadata shouldBe empty
    result.messageAction shouldBe a[MessageAction.Ignore]
    result.messageAction.message.body() shouldBe "alpakka-4"
  }

  it should "delete batch of messages" taggedAs Integration in new IntegrationFixture {
    val messages = for (i <- 0 until 10) yield s"Message - $i"
    sendMessages(messages)

    val future =
      //#batch-ack
      SqsSource(queueUrl, sqsSourceSettings)
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
    results.map(_.messageAction.message.body()) should contain theSameElementsAs messages
    verify(awsSqsClient, times(1)).deleteMessageBatch(
      any[DeleteMessageBatchRequest]
    )
  }

  it should "delete all messages in batches of given size" taggedAs Integration in new IntegrationFixture {
    val messages = for (i <- 0 until 10) yield s"Message - $i"
    sendMessages(messages)

    val future = SqsSource(queueUrl, sqsSourceSettings)
      .take(10)
      .map(MessageAction.Delete(_))
      .via(SqsAckFlow.grouped(queueUrl, SqsAckGroupedSettings().withMaxBatchSize(5)))
      .runWith(Sink.seq)

    val results = future.futureValue
    results.foreach { r =>
      r.metadata shouldBe defined
      r.messageAction shouldBe a[MessageAction.Delete]
    }
    results.map(_.messageAction.message.body()) should contain theSameElementsAs messages
    verify(awsSqsClient, times(2)).deleteMessageBatch(
      any[DeleteMessageBatchRequest]
    )
  }

  it should "fail if any of the messages in the batch request failed" in {
    val messages = for (i <- 0 until 10) yield Message.builder().body(s"Message - $i").build()

    implicit val mockAwsSqsClient = mock[SqsAsyncClient]

    when(mockAwsSqsClient.deleteMessageBatch(any[DeleteMessageBatchRequest]))
      .thenReturn(CompletableFuture.completedFuture {
        DeleteMessageBatchResponse
          .builder()
          .failed(BatchResultErrorEntry.builder().build())
          .build()
      })

    val future = Source(messages)
      .take(10)
      .map(MessageAction.Delete(_))
      .via(SqsAckFlow.grouped("queue", SqsAckGroupedSettings.Defaults))
      .runWith(Sink.ignore)
    future.failed.futureValue shouldBe a[SqsBatchException]
  }

  it should "fail if the batch request failed" in {
    val messages = for (i <- 0 until 10) yield Message.builder().body(s"Message - $i").build()

    implicit val mockAwsSqsClient = mock[SqsAsyncClient]

    when(mockAwsSqsClient.deleteMessageBatch(any[DeleteMessageBatchRequest]))
      .thenReturn(
        CompletableFuture.supplyAsync[DeleteMessageBatchResponse](new Supplier[DeleteMessageBatchResponse] {
          override def get(): DeleteMessageBatchResponse = throw new RuntimeException("Fake client error")
        })
      )

    val future = Source(messages)
      .take(10)
      .map(MessageAction.Delete(_))
      .via(SqsAckFlow.grouped("queue", SqsAckGroupedSettings.Defaults))
      .runWith(Sink.ignore)

    future.failed.futureValue shouldBe a[SqsBatchException]
  }

  it should "fail if the client invocation failed" in {
    val messages = for (i <- 0 until 10) yield Message.builder().body(s"Message - $i").build()

    implicit val mockAwsSqsClient = mock[SqsAsyncClient]

    when(
      mockAwsSqsClient.deleteMessageBatch(any[DeleteMessageBatchRequest])
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

    val future = SqsSource(queueUrl, sqsSourceSettings)
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
    results.map(_.messageAction.message.body()) should contain theSameElementsAs messages

    verify(awsSqsClient, times(1)).deleteMessageBatch(any[DeleteMessageBatchRequest])
    verify(awsSqsClient, times(1)).changeMessageVisibilityBatch(any[ChangeMessageVisibilityBatchRequest])
  }

  it should "delay batch of messages" taggedAs Integration in new IntegrationFixture {
    val messages = for (i <- 0 until 10) yield s"Message - $i"
    sendMessages(messages)

    val future =
      //#batch-requeue
      SqsSource(queueUrl, sqsSourceSettings)
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
    results.map(_.messageAction.message.body()) should contain theSameElementsAs messages
    verify(awsSqsClient, times(1))
      .changeMessageVisibilityBatch(
        any[ChangeMessageVisibilityBatchRequest]
      )
  }

  it should "ignore batch of messages" in {
    val messages = for (i <- 0 until 10) yield Message.builder().body(s"Message - $i").build()

    implicit val mockAwsSqsClient = mock[SqsAsyncClient]

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
