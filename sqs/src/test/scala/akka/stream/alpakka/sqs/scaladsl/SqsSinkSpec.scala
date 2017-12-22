/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs.scaladsl

import java.util.concurrent.{CompletableFuture, Future}

import akka.Done
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSource
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model.{SendMessageRequest, SendMessageResult}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.mockito.MockitoSugar.mock
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class SqsSinkSpec extends FlatSpec with Matchers with DefaultTestContext {

  it should "send a message" in {
    implicit val sqsClient: AmazonSQSAsync = mock[AmazonSQSAsync]
    when(sqsClient.sendMessageAsync(any[SendMessageRequest](), any())).thenAnswer(
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

    val (probe, future) = TestSource.probe[String].toMat(SqsSink("notused"))(Keep.both).run()
    probe.sendNext("notused").sendComplete()
    Await.result(future, 1.second) shouldBe Done

    verify(sqsClient, times(1)).sendMessageAsync(any[SendMessageRequest](), any)
  }

  it should "fail stage on client failure and fail the promise" in {
    implicit val sqsClient: AmazonSQSAsync = mock[AmazonSQSAsync]
    when(sqsClient.sendMessageAsync(any[SendMessageRequest](), any())).thenAnswer(
      new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock): Object = {
          invocation
            .getArgument[AsyncHandler[SendMessageRequest, SendMessageResult]](1)
            .onError(new RuntimeException("Fake client error"))
          new CompletableFuture()
        }
      }
    )

    val (probe, future) = TestSource.probe[String].toMat(SqsSink("notused"))(Keep.both).run()
    probe.sendNext("notused").sendComplete()

    a[RuntimeException] should be thrownBy {
      Await.result(future, 1.second)
    }

    verify(sqsClient, times(1)).sendMessageAsync(any[SendMessageRequest](), any)
  }

  it should "failure the promise on upstream failure" in {
    implicit val sqsClient: AmazonSQSAsync = mock[AmazonSQSAsync]
    val (probe, future) = TestSource.probe[String].toMat(SqsSink("notused"))(Keep.both).run()

    probe.sendError(new RuntimeException("Fake upstream failure"))

    a[RuntimeException] should be thrownBy {
      Await.result(future, 1.second)
    }
  }

  it should "complete promise after all messages have been sent" in {
    implicit val sqsClient: AmazonSQSAsync = mock[AmazonSQSAsync]
    when(sqsClient.sendMessageAsync(any[SendMessageRequest](), any())).thenAnswer(
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

    val (probe, future) = TestSource.probe[String].toMat(SqsSink("notused"))(Keep.both).run()
    probe
      .sendNext("test-101")
      .sendNext("test-102")
      .sendNext("test-103")
      .sendNext("test-104")
      .sendNext("test-105")
      .sendComplete()
    Await.result(future, 1.second) shouldBe Done

    verify(sqsClient, times(5)).sendMessageAsync(any[SendMessageRequest](), any)
  }
}
