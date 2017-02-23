package akka.stream.alpakka.sns

import java.util.concurrent.{CompletableFuture, Future}

import akka.Done
import akka.stream.alpakka.sns.scaladsl.SnsPublishSink
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSource
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sns.{AmazonSNSAsync, AmazonSNSAsyncClient}
import com.amazonaws.services.sns.model.{PublishRequest, PublishResult}
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.{FlatSpec, MustMatchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class SnsPublishSinkSpec extends FlatSpec with DefaultTestContext with MustMatchers {

  it should "publish a single message to sns" in {
    implicit val snsClient: AmazonSNSAsync = mock[AmazonSNSAsyncClient]
    when(snsClient.publishAsync(any[PublishRequest](), any())).thenAnswer(
      new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock): Future[PublishResult] = {
          val publishRequest = invocation.getArgument[PublishRequest](0)
          invocation
            .getArgument[AsyncHandler[PublishRequest, PublishResult]](1)
            .onSuccess(
              publishRequest,
              new PublishResult().withMessageId("message-id")
            )
          new CompletableFuture()
        }
      }
    )

    val (probe, matValue) = TestSource.probe[String].toMat(SnsPublishSink("topic-arn"))(Keep.both).run()
    probe.sendNext("sns-message").sendComplete()
    Await.result(matValue, 1.second) mustBe Done

    val expectedPublishRequest = new PublishRequest().withTopicArn("topic-arn").withMessage("sns-message")
    verify(snsClient, times(1)).publishAsync(meq(expectedPublishRequest), any())
  }

  it should "publish multiple messages to sns" in {
    implicit val snsClient: AmazonSNSAsync = mock[AmazonSNSAsyncClient]
    when(snsClient.publishAsync(any[PublishRequest](), any())).thenAnswer(
      new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock): Future[PublishResult] = {
          val publishRequest = invocation.getArgument[PublishRequest](0)
          invocation
            .getArgument[AsyncHandler[PublishRequest, PublishResult]](1)
            .onSuccess(
              publishRequest,
              new PublishResult().withMessageId("message-id")
            )
          new CompletableFuture()
        }
      }
    )

    val (probe, matValue) = TestSource.probe[String].toMat(SnsPublishSink("topic-arn"))(Keep.both).run()
    probe
      .sendNext("sns-message-1")
      .sendNext("sns-message-2")
      .sendNext("sns-message-3")
      .sendNext("sns-message-4")
      .sendNext("sns-message-5")
      .sendComplete()
    Await.result(matValue, 1.seconds) mustBe Done

    val expectedFirst = new PublishRequest().withTopicArn("topic-arn").withMessage("sns-message-1")
    verify(snsClient, times(1)).publishAsync(meq(expectedFirst), any())

    val expectedSecond = new PublishRequest().withTopicArn("topic-arn").withMessage("sns-message-2")
    verify(snsClient, times(1)).publishAsync(meq(expectedSecond), any())

    val expectedThird = new PublishRequest().withTopicArn("topic-arn").withMessage("sns-message-3")
    verify(snsClient, times(1)).publishAsync(meq(expectedThird), any())

    val expectedFourth = new PublishRequest().withTopicArn("topic-arn").withMessage("sns-message-4")
    verify(snsClient, times(1)).publishAsync(meq(expectedFourth), any())

    val expectedFifth = new PublishRequest().withTopicArn("topic-arn").withMessage("sns-message-5")
    verify(snsClient, times(1)).publishAsync(meq(expectedFifth), any())
  }

  it should "fail stage and materialized value if AmazonSNSAsyncClient request failed" in {
    implicit val snsClient: AmazonSNSAsync = mock[AmazonSNSAsyncClient]
    when(snsClient.publishAsync(any[PublishRequest](), any())).thenAnswer(
      new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock): Object = {
          invocation
            .getArgument[AsyncHandler[PublishRequest, PublishResult]](1)
            .onError(new RuntimeException("publish error"))
          new CompletableFuture()
        }
      }
    )

    val (probe, matValue) = TestSource.probe[String].toMat(SnsPublishSink("topic-arn"))(Keep.both).run()
    probe.sendNext("sns-message").sendComplete()

    a[RuntimeException] should be thrownBy {
      Await.result(matValue, 1.second)
    }

    verify(snsClient, times(1)).publishAsync(any[PublishRequest](), any())
  }

  it should "fail stage and materialized value upstream failure occurs" in {
    implicit val snsClient: AmazonSNSAsync = mock[AmazonSNSAsyncClient]

    val (probe, matValue) = TestSource.probe[String].toMat(SnsPublishSink("topic-arn"))(Keep.both).run()
    probe.sendError(new RuntimeException("upstream failure"))

    a[RuntimeException] should be thrownBy {
      Await.result(matValue, 1.second)
    }
  }

}
