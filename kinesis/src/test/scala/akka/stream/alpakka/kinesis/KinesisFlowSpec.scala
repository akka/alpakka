/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import java.util.concurrent.CompletableFuture

import akka.stream.alpakka.kinesis.KinesisErrors.FailurePublishingRecords
import akka.stream.alpakka.kinesis.scaladsl.KinesisFlow
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.util.ByteString
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model._

import scala.collection.JavaConverters._

class KinesisFlowSpec extends AnyWordSpec with Matchers with KinesisMock with LogCapturing {

  "KinesisFlow" must {

    "publish records" in assertAllStagesStopped {
      new Settings with KinesisFlowProbe with WithPutRecordsSuccess {
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

    "fail when request returns an error" in assertAllStagesStopped {
      new Settings with KinesisFlowProbe with WithPutRecordsFailure {
        sourceProbe.sendNext(record)

        sinkProbe.request(1)
        sinkProbe.expectError(FailurePublishingRecords(requestError))
      }
    }
  }

  "KinesisFlowWithUserContext" must {
    "return token in result" in assertAllStagesStopped {
      new Settings with KinesisFlowWithContextProbe with WithPutRecordsSuccess {
        val records = recordStream.take(5)
        records.foreach(sourceProbe.sendNext)
        val results = for (_ <- 1 to records.size) yield sinkProbe.requestNext()
        results shouldBe resultStream.take(records.size)

        sourceProbe.sendComplete()
        sinkProbe.expectComplete()
      }
    }
  }

  sealed trait Settings {
    val settings: KinesisFlowSettings = KinesisFlowSettings.Defaults
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
        .toMat(TestSink.probe[PutRecordsResultEntry])(Keep.both)
        .run()
  }

  trait KinesisFlowWithContextProbe { self: Settings =>
    val streamName = "stream-name"
    val recordStream = LazyList
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
    val resultStream = LazyList
      .from(1)
      .map(i => (PutRecordsResultEntry.builder().build(), i))

    val (sourceProbe, sinkProbe) =
      TestSource
        .probe[(PutRecordsRequestEntry, Int)]
        .via(KinesisFlow.withContext(streamName, settings))
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

  trait WithPutRecordsFailure { self: Settings =>
    val requestError = new RuntimeException("kinesis-error")
    when(amazonKinesisAsync.putRecords(any[PutRecordsRequest])).thenReturn({
      val future = new CompletableFuture[PutRecordsResponse]()
      future.completeExceptionally(requestError)
      future
    })
  }
}
