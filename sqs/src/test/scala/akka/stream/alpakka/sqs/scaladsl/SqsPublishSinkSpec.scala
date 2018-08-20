/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import java.util.UUID
import java.util.concurrent.{CompletableFuture, Future}

import akka.Done
import akka.stream.alpakka.sqs._
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSource
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model._
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.mockito.MockitoSugar.mock
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class SqsPublishSinkSpec extends FlatSpec with Matchers with DefaultTestContext {

  "Sqs Sink" should "send a message" in {
    implicit val sqsClient: AmazonSQSAsync = mock[AmazonSQSAsync]
    when(
      sqsClient.sendMessageAsync(any[SendMessageRequest](), any[AsyncHandler[SendMessageRequest, SendMessageResult]]())
    ).thenAnswer(
      new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock): Future[SendMessageResult] = {
          val sendMessageRequest = invocation.getArgument[SendMessageRequest](0)
          invocation
            .getArgument[AsyncHandler[SendMessageRequest, SendMessageResult]](1)
            .onSuccess(
              sendMessageRequest,
              new SendMessageResult().withMessageId(sendMessageRequest.getMessageBody)
            )
          new CompletableFuture()
        }
      }
    )

    val (probe, future) = TestSource.probe[String].toMat(SqsPublishSink("notused"))(Keep.both).run()
    probe.sendNext("notused").sendComplete()
    Await.result(future, 1.second) shouldBe Done

    verify(sqsClient, times(1))
      .sendMessageAsync(any[SendMessageRequest](), any[AsyncHandler[SendMessageRequest, SendMessageResult]]())
  }

  it should "fail stage on client failure and fail the promise" in {
    implicit val sqsClient: AmazonSQSAsync = mock[AmazonSQSAsync]
    when(
      sqsClient.sendMessageAsync(any[SendMessageRequest](), any[AsyncHandler[SendMessageRequest, SendMessageResult]]())
    ).thenAnswer(
      new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock): Object = {
          invocation
            .getArgument[AsyncHandler[SendMessageRequest, SendMessageResult]](1)
            .onError(new RuntimeException("Fake client error"))
          new CompletableFuture()
        }
      }
    )

    val (probe, future) = TestSource.probe[String].toMat(SqsPublishSink("notused"))(Keep.both).run()
    probe.sendNext("notused").sendComplete()

    a[RuntimeException] should be thrownBy {
      Await.result(future, 1.second)
    }

    verify(sqsClient, times(1))
      .sendMessageAsync(any[SendMessageRequest](), any[AsyncHandler[SendMessageRequest, SendMessageResult]]())
  }

  it should "pull a message and publish it to another queue" taggedAs Integration in {
    val queue1 = randomQueueUrl()
    val queue2 = randomQueueUrl()
    implicit val awsSqsClient = sqsClient

    sqsClient.sendMessage(queue1, "alpakka")

    val future = SqsSource(queue1, SqsSourceSettings.Defaults)
      .take(1)
      .map { m: Message =>
        m.getBody
      }
      .runWith(SqsPublishSink(queue2))
    future.futureValue shouldBe Done

    val result = sqsClient.receiveMessage(queue2)
    result.getMessages.size() shouldBe 1
    result.getMessages.get(0).getBody shouldBe "alpakka"
  }

  it should "failure the promise on upstream failure" in {
    implicit val sqsClient: AmazonSQSAsync = mock[AmazonSQSAsync]
    val (probe, future) = TestSource.probe[String].toMat(SqsPublishSink("notused"))(Keep.both).run()

    probe.sendError(new RuntimeException("Fake upstream failure"))

    a[RuntimeException] should be thrownBy {
      Await.result(future, 1.second)
    }
  }

  it should "complete promise after all messages have been sent" in {
    implicit val sqsClient: AmazonSQSAsync = mock[AmazonSQSAsync]
    when(
      sqsClient.sendMessageAsync(any[SendMessageRequest](), any[AsyncHandler[SendMessageRequest, SendMessageResult]]())
    ).thenAnswer(
      new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock): Object = {
          val sendMessageRequest = invocation.getArgument[SendMessageRequest](0)
          val callback = invocation.getArgument[AsyncHandler[SendMessageRequest, SendMessageResult]](1)
          callback.onSuccess(
            sendMessageRequest,
            new SendMessageResult().withMessageId(sendMessageRequest.getMessageBody)
          )
          new CompletableFuture()
        }
      }
    )

    val (probe, future) = TestSource.probe[String].toMat(SqsPublishSink("notused"))(Keep.both).run()
    probe
      .sendNext("test-101")
      .sendNext("test-102")
      .sendNext("test-103")
      .sendNext("test-104")
      .sendNext("test-105")
      .sendComplete()
    Await.result(future, 1.second) shouldBe Done

    verify(sqsClient, times(5))
      .sendMessageAsync(any[SendMessageRequest](), any[AsyncHandler[SendMessageRequest, SendMessageResult]]())
  }

  it should "send batch of messages" in {
    implicit val sqsClient: AmazonSQSAsync = mock[AmazonSQSAsync]
    when(
      sqsClient.sendMessageBatchAsync(any[SendMessageBatchRequest](),
                                      any[AsyncHandler[SendMessageBatchRequest, SendMessageBatchResult]]())
    ).thenAnswer(
      new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock): Future[SendMessageBatchResult] = {
          val sendMessageRequest = invocation.getArgument[SendMessageBatchRequest](0)
          invocation
            .getArgument[AsyncHandler[SendMessageBatchRequest, SendMessageBatchResult]](1)
            .onSuccess(
              sendMessageRequest,
              new SendMessageBatchResult().withSuccessful(
                new SendMessageBatchResultEntry().withId("0").withMessageId(UUID.randomUUID().toString)
              )
            )
          new CompletableFuture()
        }
      }
    )

    val (probe, future) = TestSource.probe[String].toMat(SqsPublishSink.grouped("notused"))(Keep.both).run()
    probe.sendNext("notused").sendComplete()
    Await.result(future, 1.second) shouldBe Done

    verify(sqsClient, times(1)).sendMessageBatchAsync(
      any[SendMessageBatchRequest](),
      any[AsyncHandler[SendMessageBatchRequest, SendMessageBatchResult]]()
    )
  }

  it should "send all messages in batches of given size" in {
    implicit val sqsClient: AmazonSQSAsync = mock[AmazonSQSAsync]
    when(
      sqsClient.sendMessageBatchAsync(any[SendMessageBatchRequest](),
                                      any[AsyncHandler[SendMessageBatchRequest, SendMessageBatchResult]]())
    ).thenAnswer(
      new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock): Future[SendMessageBatchResult] = {
          val sendMessageRequest = invocation.getArgument[SendMessageBatchRequest](0)
          invocation
            .getArgument[AsyncHandler[SendMessageBatchRequest, SendMessageBatchResult]](1)
            .onSuccess(
              sendMessageRequest,
              new SendMessageBatchResult().withSuccessful(
                new SendMessageBatchResultEntry().withId("0").withMessageId(UUID.randomUUID().toString),
                new SendMessageBatchResultEntry().withId("1").withMessageId(UUID.randomUUID().toString),
                new SendMessageBatchResultEntry().withId("2").withMessageId(UUID.randomUUID().toString),
                new SendMessageBatchResultEntry().withId("3").withMessageId(UUID.randomUUID().toString),
                new SendMessageBatchResultEntry().withId("4").withMessageId(UUID.randomUUID().toString)
              )
            )
          new CompletableFuture()
        }
      }
    )
    val settings = SqsPublishGroupedSettings(5, 500.millis, 1)

    val (probe, future) = TestSource.probe[String].toMat(SqsPublishSink.grouped("notused", settings))(Keep.both).run()
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

    verify(sqsClient, times(2)).sendMessageBatchAsync(
      any[SendMessageBatchRequest](),
      any[AsyncHandler[SendMessageBatchRequest, SendMessageBatchResult]]()
    )
  }

  it should "fail if any of the messages in batch failed" in {
    implicit val sqsClient: AmazonSQSAsync = mock[AmazonSQSAsync]
    when(
      sqsClient.sendMessageBatchAsync(any[SendMessageBatchRequest](),
                                      any[AsyncHandler[SendMessageBatchRequest, SendMessageBatchResult]]())
    ).thenAnswer(
      new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock): Future[SendMessageBatchResult] = {
          val sendMessageRequest = invocation.getArgument[SendMessageBatchRequest](0)
          invocation
            .getArgument[AsyncHandler[SendMessageBatchRequest, SendMessageBatchResult]](1)
            .onSuccess(
              sendMessageRequest,
              new SendMessageBatchResult()
                .withSuccessful(
                  new SendMessageBatchResultEntry().withId("0").withMessageId(UUID.randomUUID().toString),
                  new SendMessageBatchResultEntry().withId("1").withMessageId(UUID.randomUUID().toString),
                  new SendMessageBatchResultEntry().withId("3").withMessageId(UUID.randomUUID().toString),
                  new SendMessageBatchResultEntry().withId("4").withMessageId(UUID.randomUUID().toString)
                )
                .withFailed(
                  new BatchResultErrorEntry().withId("2")
                )
            )
          new CompletableFuture()
        }
      }
    )

    val (probe, future) = TestSource.probe[String].toMat(SqsPublishSink.grouped("notused"))(Keep.both).run()
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

    verify(sqsClient, times(1)).sendMessageBatchAsync(
      any[SendMessageBatchRequest](),
      any[AsyncHandler[SendMessageBatchRequest, SendMessageBatchResult]]()
    )
  }

  it should "fail if whole batch is failed" in {
    implicit val sqsClient: AmazonSQSAsync = mock[AmazonSQSAsync]
    when(
      sqsClient.sendMessageBatchAsync(any[SendMessageBatchRequest](),
                                      any[AsyncHandler[SendMessageBatchRequest, SendMessageBatchResult]]())
    ).thenAnswer(
      new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock): Future[SendMessageBatchResult] = {
          invocation
            .getArgument[AsyncHandler[SendMessageBatchRequest, SendMessageBatchResult]](1)
            .onError(new Exception("SQS Exception"))
          new CompletableFuture()
        }
      }
    )

    val settings = SqsPublishGroupedSettings(5, 500.millis, 1)
    val (probe, future) = TestSource.probe[String].toMat(SqsPublishSink.grouped("notused", settings))(Keep.both).run()
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

    verify(sqsClient, times(1)).sendMessageBatchAsync(
      any[SendMessageBatchRequest](),
      any[AsyncHandler[SendMessageBatchRequest, SendMessageBatchResult]]()
    )
  }

  it should "send all batches of messages" in {
    implicit val sqsClient: AmazonSQSAsync = mock[AmazonSQSAsync]
    when(
      sqsClient.sendMessageBatchAsync(any[SendMessageBatchRequest](),
                                      any[AsyncHandler[SendMessageBatchRequest, SendMessageBatchResult]]())
    ).thenAnswer(
      new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock): Future[SendMessageBatchResult] = {
          val sendMessageRequest = invocation.getArgument[SendMessageBatchRequest](0)
          invocation
            .getArgument[AsyncHandler[SendMessageBatchRequest, SendMessageBatchResult]](1)
            .onSuccess(
              sendMessageRequest,
              new SendMessageBatchResult().withSuccessful(
                new SendMessageBatchResultEntry().withId("0").withMessageId(UUID.randomUUID().toString),
                new SendMessageBatchResultEntry().withId("1").withMessageId(UUID.randomUUID().toString),
                new SendMessageBatchResultEntry().withId("2").withMessageId(UUID.randomUUID().toString),
                new SendMessageBatchResultEntry().withId("3").withMessageId(UUID.randomUUID().toString)
              )
            )
          new CompletableFuture()
        }
      }
    )

    val (probe, future) = TestSource.probe[Seq[String]].toMat(SqsPublishSink.batch("notused"))(Keep.both).run()
    probe
      .sendNext(
        Seq(
          "notused - 1",
          "notused - 2",
          "notused - 3",
          "notused - 4"
        )
      )
      .sendNext(
        Seq(
          "notused - 5",
          "notused - 6",
          "notused - 7",
          "notused - 8"
        )
      )
      .sendComplete()
    Await.result(future, 1.second) shouldBe Done

    verify(sqsClient, times(2)).sendMessageBatchAsync(
      any[SendMessageBatchRequest](),
      any[AsyncHandler[SendMessageBatchRequest, SendMessageBatchResult]]()
    )
  }
}
