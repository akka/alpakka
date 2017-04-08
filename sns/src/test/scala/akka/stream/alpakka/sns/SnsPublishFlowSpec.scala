/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sns

import java.util.concurrent.{CompletableFuture, Future}

import akka.stream.alpakka.sns.scaladsl.SnsPublisher
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.testkit.scaladsl.TestSource
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sns.model.{PublishRequest, PublishResult}
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

class SnsPublishFlowSpec extends FlatSpec with DefaultTestContext with MustMatchers {

  it should "publish a single message to sns" in {
    val publishRequest = new PublishRequest().withTopicArn("topic-arn").withMessage("sns-message")
    val publishResult = new PublishResult().withMessageId("message-id")

    when(snsClient.publishAsync(meq(publishRequest), any())).thenAnswer(
      new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock): Future[PublishResult] = {
          invocation
            .getArgument[AsyncHandler[PublishRequest, PublishResult]](1)
            .onSuccess(publishRequest, publishResult)
          CompletableFuture.completedFuture(publishResult)
        }
      }
    )

    val (probe, future) = TestSource.probe[String].via(SnsPublisher.flow("topic-arn")).toMat(Sink.seq)(Keep.both).run()
    probe.sendNext("sns-message").sendComplete()

    Await.result(future, 1.second) mustBe publishResult :: Nil
    verify(snsClient, times(1)).publishAsync(meq(publishRequest), any())
  }

  it should "publish multiple messages to sns" in {
    val publishResult = new PublishResult().withMessageId("message-id")

    when(snsClient.publishAsync(any[PublishRequest](), any())).thenAnswer(
      new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock): Future[PublishResult] = {
          val publishRequest = invocation.getArgument[PublishRequest](0)
          invocation
            .getArgument[AsyncHandler[PublishRequest, PublishResult]](1)
            .onSuccess(publishRequest, publishResult)
          CompletableFuture.completedFuture(publishResult)
        }
      }
    )

    val (probe, future) = TestSource.probe[String].via(SnsPublisher.flow("topic-arn")).toMat(Sink.seq)(Keep.both).run()
    probe.sendNext("sns-message-1").sendNext("sns-message-2").sendNext("sns-message-3").sendComplete()

    Await.result(future, 1.second) mustBe publishResult :: publishResult :: publishResult :: Nil

    val expectedFirst = new PublishRequest().withTopicArn("topic-arn").withMessage("sns-message-1")
    verify(snsClient, times(1)).publishAsync(meq(expectedFirst), any())

    val expectedSecond = new PublishRequest().withTopicArn("topic-arn").withMessage("sns-message-2")
    verify(snsClient, times(1)).publishAsync(meq(expectedSecond), any())

    val expectedThird = new PublishRequest().withTopicArn("topic-arn").withMessage("sns-message-3")
    verify(snsClient, times(1)).publishAsync(meq(expectedThird), any())
  }

  it should "fail stage if AmazonSNSAsyncClient request failed" in {
    val publishRequest = new PublishRequest().withTopicArn("topic-arn").withMessage("sns-message")

    when(snsClient.publishAsync(meq(publishRequest), any())).thenAnswer(
      new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock): Object = {
          invocation
            .getArgument[AsyncHandler[PublishRequest, PublishResult]](1)
            .onError(new RuntimeException("publish error"))
          val result = new CompletableFuture[PublishResult]()
          result.completeExceptionally(new RuntimeException("publish error"))
          result
        }
      }
    )

    val (probe, future) = TestSource.probe[String].via(SnsPublisher.flow("topic-arn")).toMat(Sink.seq)(Keep.both).run()
    probe.sendNext("sns-message").sendComplete()

    a[RuntimeException] should be thrownBy {
      Await.result(future, 1.second)
    }
    verify(snsClient, times(1)).publishAsync(meq(publishRequest), any())
  }

  it should "fail stage if upstream failure occurs" in {
    val (probe, future) = TestSource.probe[String].via(SnsPublisher.flow("topic-arn")).toMat(Sink.seq)(Keep.both).run()
    probe.sendError(new RuntimeException("upstream failure"))

    a[RuntimeException] should be thrownBy {
      Await.result(future, 1.second)
    }
    verify(snsClient, never()).publishAsync(any[PublishRequest](), any())
  }

}
