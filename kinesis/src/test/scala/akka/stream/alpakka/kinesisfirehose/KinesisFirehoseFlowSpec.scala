/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesisfirehose

import java.util.concurrent.CompletableFuture

import akka.stream.alpakka.kinesisfirehose.KinesisFirehoseErrors.FailurePublishingRecords
import akka.stream.alpakka.kinesisfirehose.scaladsl.KinesisFirehoseFlow
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.util.ByteString
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.firehose.model._

import scala.collection.JavaConverters._

class KinesisFirehoseFlowSpec extends AnyWordSpec with Matchers with KinesisFirehoseMock with LogCapturing {

  "KinesisFirehoseFlow" must {

    "publish records" in assertAllStagesStopped {
      new Settings with KinesisFirehoseFlowProbe with WithPutRecordsSuccess {
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

    "fails when request returns an error" in assertAllStagesStopped {
      new Settings with KinesisFirehoseFlowProbe with WithPutRecordsFailure {
        sourceProbe.sendNext(record)

        sinkProbe.request(1)
        sinkProbe.expectError(FailurePublishingRecords(requestError))
      }
    }
  }

  sealed trait Settings {
    val settings: KinesisFirehoseFlowSettings = KinesisFirehoseFlowSettings.Defaults
  }

  trait KinesisFirehoseFlowProbe { self: Settings =>
    val streamName = "stream-name"
    val record =
      Record.builder().data(SdkBytes.fromByteBuffer(ByteString("data").asByteBuffer)).build()
    val publishedRecord = PutRecordBatchResponseEntry.builder().build()
    val failingRecord =
      PutRecordBatchResponseEntry.builder().errorCode("error-code").errorMessage("error-message").build()
    val requestError = new RuntimeException("kinesisfirehose-error")

    val (sourceProbe, sinkProbe) =
      TestSource
        .probe[Record]
        .via(KinesisFirehoseFlow(streamName, settings))
        .toMat(TestSink())(Keep.both)
        .run()
  }

  trait WithPutRecordsSuccess { self: KinesisFirehoseFlowProbe =>
    when(amazonKinesisFirehoseAsync.putRecordBatch(any[PutRecordBatchRequest])).thenAnswer(new Answer[AnyRef] {
      override def answer(invocation: InvocationOnMock) = {
        val request = invocation
          .getArgument[PutRecordBatchRequest](0)
        val result = PutRecordBatchResponse
          .builder()
          .failedPutCount(0)
          .requestResponses(request.records.asScala.map(_ => publishedRecord).asJava)
          .build()
        CompletableFuture.completedFuture(result)
      }
    })
  }

  trait WithPutRecordsFailure { self: KinesisFirehoseFlowProbe =>
    when(amazonKinesisFirehoseAsync.putRecordBatch(any[PutRecordBatchRequest])).thenAnswer(new Answer[AnyRef] {
      override def answer(invocation: InvocationOnMock) = {
        val future = new CompletableFuture()
        future.completeExceptionally(requestError)
        future
      }
    })
  }

}
