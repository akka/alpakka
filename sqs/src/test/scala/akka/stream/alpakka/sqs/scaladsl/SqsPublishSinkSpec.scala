/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import java.util.UUID
import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.function.Supplier

import akka.Done
import akka.stream.alpakka.sqs._
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSource
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._

import scala.concurrent.Await
import scala.concurrent.duration._

class SqsPublishSinkSpec extends AnyFlatSpec with Matchers with DefaultTestContext with LogCapturing {

  "SqsPublishSink" should "send a message" in {
    implicit val sqsClient: SqsAsyncClient = mock[SqsAsyncClient]
    when(sqsClient.sendMessage(any[SendMessageRequest]))
      .thenReturn(CompletableFuture.completedFuture(SendMessageResponse.builder().build()))

    val (probe, future) = TestSource[String]().toMat(SqsPublishSink("notused"))(Keep.both).run()
    probe.sendNext("notused").sendComplete()
    Await.result(future, 1.second) shouldBe Done

    verify(sqsClient, times(1))
      .sendMessage(any[SendMessageRequest]())
  }

  it should "fail stage on client failure and fail the promise" in {
    implicit val sqsClient: SqsAsyncClient = mock[SqsAsyncClient]

    when(sqsClient.sendMessage(any[SendMessageRequest]()))
      .thenReturn(
        CompletableFuture.supplyAsync[SendMessageResponse](new Supplier[SendMessageResponse] {
          override def get(): SendMessageResponse = throw new RuntimeException("Fake client error")
        })
      )

    val (probe, future) = TestSource[String]().toMat(SqsPublishSink("notused"))(Keep.both).run()
    probe.sendNext("notused").sendComplete()

    a[RuntimeException] should be thrownBy {
      Await.result(future, 1.second)
    }

    verify(sqsClient, times(1))
      .sendMessage(any[SendMessageRequest]())
  }

  it should "pull a message and publish it to another queue" taggedAs Integration in {
    val queue1 = randomQueueUrl()
    val queue2 = randomQueueUrl()
    implicit val awsSqsClient = sqsClient

    val request = SendMessageRequest
      .builder()
      .queueUrl(queue1)
      .messageBody("alpakka")
      .build()

    sqsClient.sendMessage(request)

    val future = SqsSource(queue1, sqsSourceSettings)
      .take(1)
      .map(_.body())
      .runWith(SqsPublishSink(queue2))

    future.futureValue shouldBe Done

    val result =
      sqsClient.receiveMessage(ReceiveMessageRequest.builder().queueUrl(queue2).build()).get(2, TimeUnit.SECONDS)
    result.messages().size() shouldBe 1
    result.messages().get(0).body() shouldBe "alpakka"
  }

  it should "failure the promise on upstream failure" in {
    implicit val sqsClient: SqsAsyncClient = mock[SqsAsyncClient]
    val (probe, future) = TestSource[String]().toMat(SqsPublishSink("notused"))(Keep.both).run()

    probe.sendError(new RuntimeException("Fake upstream failure"))

    a[RuntimeException] should be thrownBy {
      Await.result(future, 1.second)
    }
  }

  it should "complete promise after all messages have been sent" in {
    implicit val sqsClient: SqsAsyncClient = mock[SqsAsyncClient]

    when(sqsClient.sendMessage(any[SendMessageRequest]))
      .thenReturn(CompletableFuture.completedFuture(SendMessageResponse.builder().build()))

    val (probe, future) = TestSource[String]().toMat(SqsPublishSink("notused"))(Keep.both).run()
    probe
      .sendNext("test-101")
      .sendNext("test-102")
      .sendNext("test-103")
      .sendNext("test-104")
      .sendNext("test-105")
      .sendComplete()
    Await.result(future, 1.second) shouldBe Done

    verify(sqsClient, times(5))
      .sendMessage(any[SendMessageRequest]())
  }

  it should "send batch of messages" in {
    implicit val sqsClient: SqsAsyncClient = mock[SqsAsyncClient]

    when(sqsClient.sendMessageBatch(any[SendMessageBatchRequest]))
      .thenReturn(
        CompletableFuture.completedFuture(
          SendMessageBatchResponse
            .builder()
            .successful(
              SendMessageBatchResultEntry.builder().id("0").messageId(UUID.randomUUID().toString).build()
            )
            .build()
        )
      )

    val (probe, future) = TestSource[String]().toMat(SqsPublishSink.grouped("notused"))(Keep.both).run()
    probe.sendNext("notused").sendComplete()
    Await.result(future, 1.second) shouldBe Done

    verify(sqsClient, times(1)).sendMessageBatch(
      any[SendMessageBatchRequest]()
    )
  }

  it should "send all messages in batches of given size" in {
    implicit val sqsClient: SqsAsyncClient = mock[SqsAsyncClient]

    when(sqsClient.sendMessageBatch(any[SendMessageBatchRequest]))
      .thenReturn(
        CompletableFuture.completedFuture(
          SendMessageBatchResponse
            .builder()
            .successful(
              SendMessageBatchResultEntry.builder().id("0").messageId(UUID.randomUUID().toString).build(),
              SendMessageBatchResultEntry.builder().id("1").messageId(UUID.randomUUID().toString).build(),
              SendMessageBatchResultEntry.builder().id("2").messageId(UUID.randomUUID().toString).build(),
              SendMessageBatchResultEntry.builder().id("3").messageId(UUID.randomUUID().toString).build(),
              SendMessageBatchResultEntry.builder().id("4").messageId(UUID.randomUUID().toString).build()
            )
            .build()
        )
      )

    val settings = SqsPublishGroupedSettings.create().withMaxBatchSize(5)

    val (probe, future) = TestSource[String]().toMat(SqsPublishSink.grouped("notused", settings))(Keep.both).run()
    probe
      .sendNext("notused - 1")
      .sendNext("notused - 2")
      .sendNext("notused - 3")
      .sendNext("notused - 4")
      .sendNext("notused - 5")
      .sendNext("notused - 6")
      .sendNext("notused - 7")
      .sendNext("notused - 8")
      .sendNext("notused - 9")
      .sendNext("notused - 10")
      .sendComplete()

    Await.result(future, 1.second) shouldBe Done

    verify(sqsClient, times(2)).sendMessageBatch(
      any[SendMessageBatchRequest]()
    )
  }

  it should "fail if any of the messages in batch failed" in {
    implicit val sqsClient: SqsAsyncClient = mock[SqsAsyncClient]

    when(sqsClient.sendMessageBatch(any[SendMessageBatchRequest]))
      .thenReturn(
        CompletableFuture.completedFuture(
          SendMessageBatchResponse
            .builder()
            .successful(
              SendMessageBatchResultEntry.builder().id("0").messageId(UUID.randomUUID().toString).build(),
              SendMessageBatchResultEntry.builder().id("1").messageId(UUID.randomUUID().toString).build(),
              SendMessageBatchResultEntry.builder().id("2").messageId(UUID.randomUUID().toString).build(),
              SendMessageBatchResultEntry.builder().id("3").messageId(UUID.randomUUID().toString).build()
            )
            .failed(BatchResultErrorEntry.builder().id("4").message("a very weird error just happened").build())
            .build()
        )
      )

    val (probe, future) = TestSource[String]().toMat(SqsPublishSink.grouped("notused"))(Keep.both).run()
    probe
      .sendNext("notused - 1")
      .sendNext("notused - 2")
      .sendNext("notused - 3")
      .sendNext("notused - 4")
      .sendNext("notused - 5")
      .sendComplete()

    a[SqsBatchException] should be thrownBy {
      Await.result(future, 1.second)
    }

    verify(sqsClient, times(1)).sendMessageBatch(
      any[SendMessageBatchRequest]()
    )
  }

  it should "fail if whole batch is failed" in {
    implicit val sqsClient: SqsAsyncClient = mock[SqsAsyncClient]
    when(
      sqsClient.sendMessageBatch(any[SendMessageBatchRequest]())
    ).thenReturn(
      CompletableFuture.supplyAsync[SendMessageBatchResponse](new Supplier[SendMessageBatchResponse] {
        override def get(): SendMessageBatchResponse = throw new RuntimeException("Fake client error")
      })
    )

    val settings = SqsPublishGroupedSettings().withMaxBatchSize(5)
    val (probe, future) = TestSource[String]().toMat(SqsPublishSink.grouped("notused", settings))(Keep.both).run()
    probe
      .sendNext("notused - 1")
      .sendNext("notused - 2")
      .sendNext("notused - 3")
      .sendNext("notused - 4")
      .sendNext("notused - 5")
      .sendComplete()

    a[RuntimeException] should be thrownBy {
      Await.result(future, 1.second)
    }

    verify(sqsClient, times(1)).sendMessageBatch(
      any[SendMessageBatchRequest]()
    )
  }

  it should "send all batches of messages" in {
    implicit val sqsClient: SqsAsyncClient = mock[SqsAsyncClient]

    when(sqsClient.sendMessageBatch(any[SendMessageBatchRequest]))
      .thenReturn(
        CompletableFuture.completedFuture(
          SendMessageBatchResponse
            .builder()
            .successful(
              SendMessageBatchResultEntry.builder().id("0").messageId(UUID.randomUUID().toString).build(),
              SendMessageBatchResultEntry.builder().id("1").messageId(UUID.randomUUID().toString).build(),
              SendMessageBatchResultEntry.builder().id("2").messageId(UUID.randomUUID().toString).build(),
              SendMessageBatchResultEntry.builder().id("3").messageId(UUID.randomUUID().toString).build()
            )
            .build()
        )
      )

    val (probe, future) = TestSource[Seq[String]]().toMat(SqsPublishSink.batch("notused"))(Keep.both).run()
    probe
      .sendNext(
        Seq(
          "notused - 0",
          "notused - 1",
          "notused - 2",
          "notused - 3"
        )
      )
      .sendNext(
        Seq(
          "notused - 4",
          "notused - 5",
          "notused - 6",
          "notused - 7"
        )
      )
      .sendComplete()
    Await.result(future, 1.second) shouldBe Done

    verify(sqsClient, times(2)).sendMessageBatch(
      any[SendMessageBatchRequest]()
    )
  }
}
