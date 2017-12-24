/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import java.util.concurrent.CompletableFuture

import akka.stream.alpakka.kinesis.KinesisErrors.{ErrorPublishingRecords, FailurePublishingRecords}
import akka.stream.alpakka.kinesis.scaladsl.KinesisFlow
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.util.ByteString
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.kinesis.model._
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.{Matchers, WordSpecLike}

import scala.collection.JavaConverters._

class KinesisFlowSpec extends WordSpecLike with Matchers with DefaultTestContext {

  "KinesisFlow" must {

    "publish records" in new DefaultSettings with KinesisFlowProbe with WithPutRecordsSuccess {
      sourceProbe.sendNext(record)
      sourceProbe.sendNext(record)
      sourceProbe.sendNext(record)
      sourceProbe.sendNext(record)
      sourceProbe.sendNext(record)

      sinkProbe.requestNext() shouldBe publishedRecord
      sinkProbe.requestNext() shouldBe publishedRecord
      sinkProbe.requestNext() shouldBe publishedRecord
      sinkProbe.requestNext() shouldBe publishedRecord
      sinkProbe.requestNext() shouldBe publishedRecord

      sourceProbe.sendComplete()
      sinkProbe.expectComplete()
    }
    "publish records with retries" in new DefaultSettings with KinesisFlowProbe {
      when(amazonKinesisAsync.putRecordsAsync(any(), any()))
        .thenAnswer(new Answer[AnyRef] {
          override def answer(invocation: InvocationOnMock) = {
            val request = invocation
              .getArgument[PutRecordsRequest](0)
            val result = new PutRecordsResult()
              .withFailedRecordCount(request.getRecords.size())
              .withRecords(
                request.getRecords.asScala
                  .map(_ => failingRecord)
                  .asJava
              )
            invocation
              .getArgument[AsyncHandler[PutRecordsRequest, PutRecordsResult]](1)
              .onSuccess(request, result)
            CompletableFuture.completedFuture(result)
          }
        })
        .thenAnswer(new Answer[AnyRef] {
          override def answer(invocation: InvocationOnMock) = {
            val request = invocation
              .getArgument[PutRecordsRequest](0)
            val result = new PutRecordsResult()
              .withFailedRecordCount(0)
              .withRecords(request.getRecords.asScala.map(_ => publishedRecord).asJava)
            invocation
              .getArgument[AsyncHandler[PutRecordsRequest, PutRecordsResult]](1)
              .onSuccess(request, result)
            CompletableFuture.completedFuture(result)
          }
        })

      sourceProbe.sendNext(record)

      sinkProbe.requestNext(settings.retryInitialTimeout * 2) shouldBe publishedRecord

      sourceProbe.sendComplete()
      sinkProbe.expectComplete()
    }
    "fail after trying to publish records with no retires" in new NoRetries with KinesisFlowProbe
    with WithPutRecordsWithPartialErrors {
      sourceProbe.sendNext(record)

      sinkProbe.request(1)
      sinkProbe.expectError(ErrorPublishingRecords(1, Seq(failingRecord)))
    }
    "fail after trying to publish records with several retries" in new DefaultSettings with KinesisFlowProbe
    with WithPutRecordsWithPartialErrors {
      sourceProbe.sendNext(record)

      sinkProbe.request(1)
      sinkProbe.expectError(ErrorPublishingRecords(settings.maxRetries + 1, Seq(failingRecord)))
    }
    "fails when request returns an error" in new DefaultSettings with KinesisFlowProbe with WithPutRecordsFailure {
      sourceProbe.sendNext(record)

      sinkProbe.request(1)
      sinkProbe.expectError(FailurePublishingRecords(requestError))
    }
  }

  sealed trait Settings {
    val settings: KinesisFlowSettings
  }
  trait DefaultSettings extends Settings {
    val settings = KinesisFlowSettings.defaultInstance.copy(maxRetries = 1)
  }
  trait NoRetries extends Settings {
    val settings = KinesisFlowSettings.defaultInstance.copy(maxRetries = 0)
  }

  trait KinesisFlowProbe { self: Settings =>
    val streamName = "stream-name"
    val record =
      new PutRecordsRequestEntry().withPartitionKey("partition-key").withData(ByteString("data").asByteBuffer)
    val publishedRecord = new PutRecordsResultEntry()
    val failingRecord = new PutRecordsResultEntry().withErrorCode("error-code").withErrorMessage("error-message")
    val requestError = new RuntimeException("kinesis-error")

    val (sourceProbe, sinkProbe) =
      TestSource
        .probe[PutRecordsRequestEntry]
        .via(KinesisFlow(streamName, settings))
        .toMat(TestSink.probe)(Keep.both)
        .run()
  }

  trait WithPutRecordsSuccess { self: KinesisFlowProbe =>
    when(amazonKinesisAsync.putRecordsAsync(any(), any())).thenAnswer(new Answer[AnyRef] {
      override def answer(invocation: InvocationOnMock) = {
        val request = invocation
          .getArgument[PutRecordsRequest](0)
        val result = new PutRecordsResult()
          .withFailedRecordCount(0)
          .withRecords(request.getRecords.asScala.map(_ => publishedRecord).asJava)
        invocation
          .getArgument[AsyncHandler[PutRecordsRequest, PutRecordsResult]](1)
          .onSuccess(request, result)
        CompletableFuture.completedFuture(result)
      }
    })
  }

  trait WithPutRecordsWithPartialErrors { self: KinesisFlowProbe =>
    when(amazonKinesisAsync.putRecordsAsync(any(), any()))
      .thenAnswer(new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock) = {
          val request = invocation
            .getArgument[PutRecordsRequest](0)
          val result = new PutRecordsResult()
            .withFailedRecordCount(request.getRecords.size())
            .withRecords(
              request.getRecords.asScala
                .map(_ => failingRecord)
                .asJava
            )
          invocation
            .getArgument[AsyncHandler[PutRecordsRequest, PutRecordsResult]](1)
            .onSuccess(request, result)
          CompletableFuture.completedFuture(result)
        }
      })
  }

  trait WithPutRecordsFailure { self: KinesisFlowProbe =>
    when(amazonKinesisAsync.putRecordsAsync(any(), any())).thenAnswer(new Answer[AnyRef] {
      override def answer(invocation: InvocationOnMock) = {
        invocation
          .getArgument[AsyncHandler[PutRecordsRequest, PutRecordsResult]](1)
          .onError(requestError)
        val future = new CompletableFuture()
        future.completeExceptionally(requestError)
        future
      }
    })
  }

}
