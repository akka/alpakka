/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import java.util.concurrent.CompletableFuture

import akka.stream.alpakka.kinesis.KinesisErrors.{ErrorPublishingRecords, FailurePublishingRecords}
import akka.stream.alpakka.kinesis.scaladsl.KinesisFlow
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.util.ByteString
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.{Matchers, WordSpecLike}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model._

import scala.collection.JavaConverters._

class KinesisFlowSpec extends WordSpecLike with Matchers with KinesisMock {

  "KinesisFlow" must {

    "publish records" in assertAllStagesStopped {
      new DefaultSettings with KinesisFlowProbe with WithPutRecordsSuccess {
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
      new DefaultSettings with KinesisFlowProbe with WithPutRecordsInitialErrorsSuccessfulRetry {
        sourceProbe.sendNext(record)

        sinkProbe.requestNext(settings.retryInitialTimeout * 2) shouldBe publishedRecord

        sourceProbe.sendComplete()
        sinkProbe.expectComplete()
      }
    }

    "fail after trying to publish records with no retries" in assertAllStagesStopped {
      new NoRetries with KinesisFlowProbe with WithPutRecordsWithPartialErrors {
        sourceProbe.sendNext(record)

        sinkProbe.request(1)
        sinkProbe.expectError(ErrorPublishingRecords(1, Seq((failingRecord, ()))))
      }
    }

    "fail after trying to publish records with several retries" in assertAllStagesStopped {
      new DefaultSettings with KinesisFlowProbe with WithPutRecordsWithPartialErrors {
        sourceProbe.sendNext(record)

        sinkProbe.request(1)
        sinkProbe.expectError(ErrorPublishingRecords(settings.maxRetries + 1, Seq((failingRecord, ()))))
      }
    }

    "fail when request returns an error" in assertAllStagesStopped {
      new DefaultSettings with KinesisFlowProbe with WithPutRecordsFailure {
        sourceProbe.sendNext(record)

        sinkProbe.request(1)
        sinkProbe.expectError(FailurePublishingRecords(requestError))
      }
    }
  }

  "KinesisFlowWithUserContext" must {
    "return token in result" in assertAllStagesStopped {
      new DefaultSettings with KinesisFlowWithUserContextProbe with WithPutRecordsSuccess {
        val records = recordStream.take(5)
        records.foreach(sourceProbe.sendNext)
        val results = for (_ <- 1 to records.size) yield sinkProbe.requestNext()
        results shouldBe resultStream.take(records.size)

        sourceProbe.sendComplete()
        sinkProbe.expectComplete()
      }
    }

    "return token in retried result" in assertAllStagesStopped {
      new DefaultSettings with KinesisFlowWithUserContextProbe with WithPutRecordsInitialErrorsSuccessfulRetry {
        val record = recordStream.take(1).head
        sourceProbe.sendNext(record)

        sinkProbe.requestNext(settings.retryInitialTimeout * 2) shouldBe resultStream.take(1).head

        sourceProbe.sendComplete()
        sinkProbe.expectComplete()
      }
    }
  }

  sealed trait Settings {
    val settings: KinesisFlowSettings
  }
  trait DefaultSettings extends Settings {
    val settings = KinesisFlowSettings.Defaults.withMaxRetries(1)
  }
  trait NoRetries extends Settings {
    val settings = KinesisFlowSettings.Defaults.withMaxRetries(0)
  }

  trait KinesisFlowProbe { self: Settings =>
    val streamName = "stream-name"
    val record =
      PutRecordsRequestEntry
        .builder()
        .partitionKey("partition-key")
        .data(SdkBytes.fromByteBuffer(ByteString("data").asByteBuffer))
        .build()

    val (sourceProbe, sinkProbe) =
      TestSource
        .probe[PutRecordsRequestEntry]
        .via(KinesisFlow(streamName, settings))
        .toMat(TestSink.probe)(Keep.both)
        .run()
  }

  trait KinesisFlowWithUserContextProbe { self: Settings =>
    val streamName = "stream-name"
    val recordStream = Stream
      .from(1)
      .map(
        i =>
          (PutRecordsRequestEntry
             .builder()
             .partitionKey("partition-key")
             .data(SdkBytes.fromByteBuffer(ByteString(i).asByteBuffer))
             .build(),
           i)
      )
    val resultStream = Stream
      .from(1)
      .map(i => (PutRecordsResultEntry.builder().build(), i))

    val (sourceProbe, sinkProbe) =
      TestSource
        .probe[(PutRecordsRequestEntry, Int)]
        .via(KinesisFlow.withUserContext(streamName, settings))
        .toMat(TestSink.probe)(Keep.both)
        .run()
  }

  trait WithPutRecordsSuccess { self: Settings =>
    val publishedRecord = PutRecordsResultEntry.builder().build()
    when(amazonKinesisAsync.putRecords(any[PutRecordsRequest])).thenAnswer(new Answer[AnyRef] {
      override def answer(invocation: InvocationOnMock) = {
        val request = invocation
          .getArgument[PutRecordsRequest](0)
        val result = PutRecordsResponse
          .builder()
          .failedRecordCount(0)
          .records(request.records.asScala.map(_ => publishedRecord).asJava)
          .build()
        CompletableFuture.completedFuture(result)
      }
    })
  }

  trait WithPutRecordsWithPartialErrors { self: Settings =>
    val failingRecord = PutRecordsResultEntry.builder().errorCode("error-code").errorMessage("error-message").build()
    when(amazonKinesisAsync.putRecords(any[PutRecordsRequest]))
      .thenAnswer(new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock) = {
          val request = invocation
            .getArgument[PutRecordsRequest](0)
          val result = PutRecordsResponse
            .builder()
            .failedRecordCount(request.records.size())
            .records(
              request.records.asScala
                .map(_ => failingRecord)
                .asJava
            )
            .build()
          CompletableFuture.completedFuture(result)
        }
      })
  }

  trait WithPutRecordsInitialErrorsSuccessfulRetry { self: Settings =>
    val publishedRecord = PutRecordsResultEntry.builder().build()
    val failingRecord = PutRecordsResultEntry.builder().errorCode("error-code").errorMessage("error-message").build()
    when(amazonKinesisAsync.putRecords(any[PutRecordsRequest]))
      .thenAnswer(new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock) = {
          val request = invocation
            .getArgument[PutRecordsRequest](0)
          val result = PutRecordsResponse
            .builder()
            .failedRecordCount(request.records.size())
            .records(
              request.records.asScala
                .map(_ => failingRecord)
                .asJava
            )
            .build()
          CompletableFuture.completedFuture(result)
        }
      })
      .thenAnswer(new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock) = {
          val request = invocation
            .getArgument[PutRecordsRequest](0)
          val result = PutRecordsResponse
            .builder()
            .failedRecordCount(0)
            .records(request.records.asScala.map(_ => publishedRecord).asJava)
            .build()
          CompletableFuture.completedFuture(result)
        }
      })
  }

  trait WithPutRecordsFailure { self: Settings =>
    val requestError = new RuntimeException("kinesis-error")
    when(amazonKinesisAsync.putRecords(any[PutRecordsRequest])).thenReturn({
      val future = new CompletableFuture[PutRecordsResponse]()
      future.completeExceptionally(requestError)
      future
    })
  }
}
