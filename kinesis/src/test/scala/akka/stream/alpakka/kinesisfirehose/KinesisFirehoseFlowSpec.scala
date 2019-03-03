/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesisfirehose

import java.util.concurrent.CompletableFuture

import akka.stream.alpakka.kinesisfirehose.KinesisFirehoseErrors.{ErrorPublishingRecords, FailurePublishingRecords}
import akka.stream.alpakka.kinesisfirehose.scaladsl.KinesisFirehoseFlow
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.util.ByteString
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.kinesisfirehose.model._
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.{Matchers, WordSpecLike}

import scala.collection.JavaConverters._

class KinesisFirehoseFlowSpec extends WordSpecLike with Matchers with KinesisFirehoseMock {

  "KinesisFirehoseFlow" must {

    "publish records" in assertAllStagesStopped {
      new DefaultSettings with KinesisFirehoseFlowProbe with WithPutRecordsSuccess {
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
    }

    "publish records with retries" in assertAllStagesStopped {
      new DefaultSettings with KinesisFirehoseFlowProbe {
        when(amazonKinesisFirehoseAsync.putRecordBatchAsync(any(), any()))
          .thenAnswer(new Answer[AnyRef] {
            override def answer(invocation: InvocationOnMock) = {
              val request = invocation
                .getArgument[PutRecordBatchRequest](0)
              val result = new PutRecordBatchResult()
                .withFailedPutCount(request.getRecords.size())
                .withRequestResponses(
                  request.getRecords.asScala
                    .map(_ => failingRecord)
                    .asJava
                )
              invocation
                .getArgument[AsyncHandler[PutRecordBatchRequest, PutRecordBatchResult]](1)
                .onSuccess(request, result)
              CompletableFuture.completedFuture(result)
            }
          })
          .thenAnswer(new Answer[AnyRef] {
            override def answer(invocation: InvocationOnMock) = {
              val request = invocation
                .getArgument[PutRecordBatchRequest](0)
              val result = new PutRecordBatchResult()
                .withFailedPutCount(0)
                .withRequestResponses(request.getRecords.asScala.map(_ => publishedRecord).asJava)
              invocation
                .getArgument[AsyncHandler[PutRecordBatchRequest, PutRecordBatchResult]](1)
                .onSuccess(request, result)
              CompletableFuture.completedFuture(result)
            }
          })

        sourceProbe.sendNext(record)

        sinkProbe.requestNext(settings.retryInitialTimeout * 2) shouldBe publishedRecord

        sourceProbe.sendComplete()
        sinkProbe.expectComplete()
      }
    }

    "fail after trying to publish records with no retires" in assertAllStagesStopped {
      new NoRetries with KinesisFirehoseFlowProbe with WithPutRecordsWithPartialErrors {
        sourceProbe.sendNext(record)

        sinkProbe.request(1)
        sinkProbe.expectError(ErrorPublishingRecords(1, Seq(failingRecord)))
      }
    }

    "fail after trying to publish records with several retries" in assertAllStagesStopped {
      new DefaultSettings with KinesisFirehoseFlowProbe with WithPutRecordsWithPartialErrors {
        sourceProbe.sendNext(record)

        sinkProbe.request(1)
        sinkProbe.expectError(ErrorPublishingRecords(settings.maxRetries + 1, Seq(failingRecord)))
      }
    }

    "fails when request returns an error" in assertAllStagesStopped {
      new DefaultSettings with KinesisFirehoseFlowProbe with WithPutRecordsFailure {
        sourceProbe.sendNext(record)

        sinkProbe.request(1)
        sinkProbe.expectError(FailurePublishingRecords(requestError))
      }
    }
  }

  sealed trait Settings {
    val settings: KinesisFirehoseFlowSettings
  }
  trait DefaultSettings extends Settings {
    val settings = KinesisFirehoseFlowSettings.Defaults.withMaxRetries(1)
  }
  trait NoRetries extends Settings {
    val settings = KinesisFirehoseFlowSettings.Defaults.withMaxRetries(0)
  }

  trait KinesisFirehoseFlowProbe { self: Settings =>
    val streamName = "stream-name"
    val record =
      new Record().withData(ByteString("data").asByteBuffer)
    val publishedRecord = new PutRecordBatchResponseEntry()
    val failingRecord = new PutRecordBatchResponseEntry().withErrorCode("error-code").withErrorMessage("error-message")
    val requestError = new RuntimeException("kinesisfirehose-error")

    val (sourceProbe, sinkProbe) =
      TestSource
        .probe[Record]
        .via(KinesisFirehoseFlow(streamName, settings))
        .toMat(TestSink.probe)(Keep.both)
        .run()
  }

  trait WithPutRecordsSuccess { self: KinesisFirehoseFlowProbe =>
    when(amazonKinesisFirehoseAsync.putRecordBatchAsync(any(), any())).thenAnswer(new Answer[AnyRef] {
      override def answer(invocation: InvocationOnMock) = {
        val request = invocation
          .getArgument[PutRecordBatchRequest](0)
        val result = new PutRecordBatchResult()
          .withFailedPutCount(0)
          .withRequestResponses(request.getRecords.asScala.map(_ => publishedRecord).asJava)
        invocation
          .getArgument[AsyncHandler[PutRecordBatchRequest, PutRecordBatchResult]](1)
          .onSuccess(request, result)
        CompletableFuture.completedFuture(result)
      }
    })
  }

  trait WithPutRecordsWithPartialErrors { self: KinesisFirehoseFlowProbe =>
    when(amazonKinesisFirehoseAsync.putRecordBatchAsync(any(), any()))
      .thenAnswer(new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock) = {
          val request = invocation
            .getArgument[PutRecordBatchRequest](0)
          val result = new PutRecordBatchResult()
            .withFailedPutCount(request.getRecords.size())
            .withRequestResponses(
              request.getRecords.asScala
                .map(_ => failingRecord)
                .asJava
            )
          invocation
            .getArgument[AsyncHandler[PutRecordBatchRequest, PutRecordBatchResult]](1)
            .onSuccess(request, result)
          CompletableFuture.completedFuture(result)
        }
      })
  }

  trait WithPutRecordsFailure { self: KinesisFirehoseFlowProbe =>
    when(amazonKinesisFirehoseAsync.putRecordBatchAsync(any(), any())).thenAnswer(new Answer[AnyRef] {
      override def answer(invocation: InvocationOnMock) = {
        invocation
          .getArgument[AsyncHandler[PutRecordBatchRequest, PutRecordBatchResult]](1)
          .onError(requestError)
        val future = new CompletableFuture()
        future.completeExceptionally(requestError)
        future
      }
    })
  }

}
