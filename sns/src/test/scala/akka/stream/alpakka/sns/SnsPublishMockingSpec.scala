/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.sns

import java.util.concurrent.CompletableFuture

import akka.stream.alpakka.sns.scaladsl.SnsPublisher
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.testkit.scaladsl.TestSource
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import software.amazon.awssdk.services.sns.model.{PublishRequest, PublishResponse}

import scala.concurrent.Await
import scala.concurrent.duration._

class SnsPublishMockingSpec extends AnyFlatSpec with DefaultTestContext with Matchers with LogCapturing {

  it should "publish a single PublishRequest message to sns" in {
    val publishRequest = PublishRequest.builder().topicArn("topic-arn").message("sns-message").build()
    val publishResult = PublishResponse.builder().messageId("message-id").build()

    when(snsClient.publish(meq(publishRequest))).thenReturn(CompletableFuture.completedFuture(publishResult))

    val (probe, future) =
      TestSource.probe[PublishRequest].via(SnsPublisher.publishFlow("topic-arn")).toMat(Sink.seq)(Keep.both).run()

    probe.sendNext(PublishRequest.builder().message("sns-message").build()).sendComplete()

    Await.result(future, 1.second) mustBe publishResult :: Nil

    verify(snsClient, times(1)).publish(meq(publishRequest))
  }

  it should "publish multiple PublishRequest messages to sns" in {
    val publishResult = PublishResponse.builder().messageId("message-id").build()

    when(snsClient.publish(any[PublishRequest]())).thenReturn(CompletableFuture.completedFuture(publishResult))

    val (probe, future) =
      TestSource.probe[PublishRequest].via(SnsPublisher.publishFlow("topic-arn")).toMat(Sink.seq)(Keep.both).run()

    probe
      .sendNext(PublishRequest.builder().message("sns-message-1").build())
      .sendNext(PublishRequest.builder().message("sns-message-2").build())
      .sendNext(PublishRequest.builder().message("sns-message-3").build())
      .sendComplete()

    Await.result(future, 1.second) mustBe publishResult :: publishResult :: publishResult :: Nil

    val expectedFirst = PublishRequest.builder().message("sns-message-1").topicArn("topic-arn").build()
    verify(snsClient, times(1)).publish(meq(expectedFirst))

    val expectedSecond = PublishRequest.builder().message("sns-message-2").topicArn("topic-arn").build()
    verify(snsClient, times(1)).publish(meq(expectedSecond))

    val expectedThird = PublishRequest.builder().message("sns-message-3").topicArn("topic-arn").build()
    verify(snsClient, times(1)).publish(meq(expectedThird))
  }

  it should "publish multiple PublishRequest messages to multiple sns topics" in {
    val publishResult = PublishResponse.builder().messageId("message-id").build()

    when(snsClient.publish(any[PublishRequest]())).thenReturn(CompletableFuture.completedFuture(publishResult))

    val (probe, future) =
      TestSource.probe[PublishRequest].via(SnsPublisher.publishFlow()).toMat(Sink.seq)(Keep.both).run()

    probe
      .sendNext(PublishRequest.builder().message("sns-message-1").topicArn("topic-arn-1").build())
      .sendNext(PublishRequest.builder().message("sns-message-2").topicArn("topic-arn-2").build())
      .sendNext(PublishRequest.builder().message("sns-message-3").topicArn("topic-arn-3").build())
      .sendComplete()

    Await.result(future, 1.second) mustBe publishResult :: publishResult :: publishResult :: Nil

    val expectedFirst = PublishRequest.builder().message("sns-message-1").topicArn("topic-arn-1").build()
    verify(snsClient, times(1)).publish(meq(expectedFirst))

    val expectedSecond = PublishRequest.builder().message("sns-message-2").topicArn("topic-arn-2").build()
    verify(snsClient, times(1)).publish(meq(expectedSecond))

    val expectedThird = PublishRequest.builder().message("sns-message-3").topicArn("topic-arn-3").build()
    verify(snsClient, times(1)).publish(meq(expectedThird))
  }

  it should "publish a single string message to sns" in {
    val publishRequest = PublishRequest.builder().topicArn("topic-arn").message("sns-message").build()
    val publishResult = PublishResponse.builder().messageId("message-id").build()

    when(snsClient.publish(meq(publishRequest))).thenReturn(CompletableFuture.completedFuture(publishResult))

    val (probe, future) = TestSource.probe[String].via(SnsPublisher.flow("topic-arn")).toMat(Sink.seq)(Keep.both).run()
    probe.sendNext("sns-message").sendComplete()

    Await.result(future, 1.second) mustBe publishResult :: Nil
    verify(snsClient, times(1)).publish(meq(publishRequest))
  }

  it should "publish multiple string messages to sns" in {
    val publishResult = PublishResponse.builder().messageId("message-id").build()

    when(snsClient.publish(any[PublishRequest]())).thenReturn(CompletableFuture.completedFuture(publishResult))

    val (probe, future) = TestSource.probe[String].via(SnsPublisher.flow("topic-arn")).toMat(Sink.seq)(Keep.both).run()
    probe.sendNext("sns-message-1").sendNext("sns-message-2").sendNext("sns-message-3").sendComplete()

    Await.result(future, 1.second) mustBe publishResult :: publishResult :: publishResult :: Nil

    val expectedFirst = PublishRequest.builder().message("sns-message-1").topicArn("topic-arn").build()
    verify(snsClient, times(1)).publish(meq(expectedFirst))

    val expectedSecond = PublishRequest.builder().message("sns-message-2").topicArn("topic-arn").build()
    verify(snsClient, times(1)).publish(meq(expectedSecond))

    val expectedThird = PublishRequest.builder().message("sns-message-3").topicArn("topic-arn").build()
    verify(snsClient, times(1)).publish(meq(expectedThird))
  }

  it should "fail stage if AmazonSNSAsyncClient request failed" in {
    val publishRequest = PublishRequest.builder().topicArn("topic-arn").message("sns-message").build()

    val promise = new CompletableFuture[PublishResponse]()
    promise.completeExceptionally(new RuntimeException("publish error"))

    when(snsClient.publish(meq(publishRequest))).thenReturn(promise)

    val (probe, future) = TestSource.probe[String].via(SnsPublisher.flow("topic-arn")).toMat(Sink.seq)(Keep.both).run()
    probe.sendNext("sns-message").sendComplete()

    a[RuntimeException] should be thrownBy {
      Await.result(future, 1.second)
    }

    verify(snsClient, times(1)).publish(meq(publishRequest))
  }

  it should "fail stage if upstream failure occurs" in {
    case class MyCustomException(message: String) extends Exception(message)

    val (probe, future) = TestSource.probe[String].via(SnsPublisher.flow("topic-arn")).toMat(Sink.seq)(Keep.both).run()
    probe.sendError(MyCustomException("upstream failure"))

    a[MyCustomException] should be thrownBy {
      Await.result(future, 1.second)
    }

    verify(snsClient, never()).publish(any[PublishRequest]())
  }

}
