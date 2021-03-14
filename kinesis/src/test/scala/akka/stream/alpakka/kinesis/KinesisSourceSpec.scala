/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import java.util
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicReference

import akka.stream.alpakka.kinesis.scaladsl.KinesisSource
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.util.ByteString
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model._

import scala.concurrent.duration._

class KinesisSourceSpec extends AnyWordSpec with Matchers with KinesisMock with LogCapturing {

  implicit class recordToString(r: Record) {
    def utf8String: String = ByteString(r.data.asByteBuffer).utf8String
  }

  "KinesisSource" must {

    val shardSettings =
      ShardSettings("stream_name", "shard-id")
        .withShardIterator(ShardIterator.TrimHorizon)

    "poll for records" in assertAllStagesStopped {
      new KinesisSpecContext with WithGetShardIteratorSuccess with WithGetRecordsSuccess {
        override def shards: util.List[Shard] = util.Arrays.asList(Shard.builder().shardId("id").build())

        override def records = util.Arrays.asList(
          Record.builder().data(SdkBytes.fromByteBuffer(ByteString("1").toByteBuffer)).build(),
          Record.builder().data(SdkBytes.fromByteBuffer(ByteString("2").toByteBuffer)).build()
        )

        val probe = KinesisSource.basic(shardSettings, amazonKinesisAsync).runWith(TestSink.probe)

        probe.requestNext.utf8String shouldEqual "1"
        probe.requestNext.utf8String shouldEqual "2"
        probe.requestNext.utf8String shouldEqual "1"
        probe.requestNext.utf8String shouldEqual "2"
        probe.cancel()
      }
    }

    "poll for records with mutliple requests" in assertAllStagesStopped {
      new KinesisSpecContext with WithGetShardIteratorSuccess with WithGetRecordsSuccess {
        override def shards: util.List[Shard] = util.Arrays.asList(Shard.builder().shardId("id").build())

        override def records = util.Arrays.asList(
          Record.builder().data(SdkBytes.fromByteBuffer(ByteString("1").toByteBuffer)).build(),
          Record.builder().data(SdkBytes.fromByteBuffer(ByteString("2").toByteBuffer)).build()
        )

        val probe = KinesisSource.basic(shardSettings, amazonKinesisAsync).runWith(TestSink.probe)

        probe.request(2)
        probe.expectNext().utf8String shouldEqual "1"
        probe.expectNext().utf8String shouldEqual "2"
        probe.expectNoMessage(1.second)
        probe.cancel()
      }
    }

    "wait for request before passing downstream" in assertAllStagesStopped {
      new KinesisSpecContext with WithGetShardIteratorSuccess with WithGetRecordsSuccess {
        override def shards: util.List[Shard] = util.Arrays.asList(Shard.builder().shardId("id").build())

        override def records = util.Arrays.asList(
          Record.builder().data(SdkBytes.fromByteBuffer(ByteString("1").toByteBuffer)).build(),
          Record.builder().data(SdkBytes.fromByteBuffer(ByteString("2").toByteBuffer)).build(),
          Record.builder().data(SdkBytes.fromByteBuffer(ByteString("3").toByteBuffer)).build(),
          Record.builder().data(SdkBytes.fromByteBuffer(ByteString("4").toByteBuffer)).build(),
          Record.builder().data(SdkBytes.fromByteBuffer(ByteString("5").toByteBuffer)).build(),
          Record.builder().data(SdkBytes.fromByteBuffer(ByteString("6").toByteBuffer)).build()
        )

        val probe = KinesisSource.basic(shardSettings, amazonKinesisAsync).runWith(TestSink.probe)

        probe.request(1)
        probe.expectNext().utf8String shouldEqual "1"
        probe.expectNoMessage(1.second)
        probe.requestNext().utf8String shouldEqual "2"
        probe.requestNext().utf8String shouldEqual "3"
        probe.requestNext().utf8String shouldEqual "4"
        probe.requestNext().utf8String shouldEqual "5"
        probe.requestNext().utf8String shouldEqual "6"
        probe.requestNext().utf8String shouldEqual "1"
        probe.cancel()
      }
    }

    "merge multiple shards" in assertAllStagesStopped {
      new KinesisSpecContext with WithGetShardIteratorSuccess with WithGetRecordsSuccess {
        val mergeSettings = List(
          shardSettings.withShardId("0"),
          shardSettings.withShardId("1")
        )

        override def shards: util.List[Shard] =
          util.Arrays.asList(Shard.builder().shardId("1").build(), Shard.builder().shardId("2").build())

        override def records = util.Arrays.asList(
          Record.builder().data(SdkBytes.fromByteBuffer(ByteString("1").toByteBuffer)).build(),
          Record.builder().data(SdkBytes.fromByteBuffer(ByteString("2").toByteBuffer)).build(),
          Record.builder().data(SdkBytes.fromByteBuffer(ByteString("3").toByteBuffer)).build()
        )

        val probe =
          KinesisSource.basicMerge(mergeSettings, amazonKinesisAsync).map(_.utf8String).runWith(TestSink.probe)

        probe.request(6)
        probe.expectNextUnordered("1", "1", "2", "2", "3", "3")
        probe.cancel()
      }
    }

    "complete stage when next shard iterator is null" in assertAllStagesStopped {
      new KinesisSpecContext with WithGetShardIteratorSuccess with WithGetRecordsSuccess {
        override def records =
          util.Arrays.asList(Record.builder().data(SdkBytes.fromByteBuffer(ByteString("1").toByteBuffer)).build())

        val probe = KinesisSource.basic(shardSettings, amazonKinesisAsync).runWith(TestSink.probe)

        probe.requestNext.utf8String shouldEqual "1"
        nextShardIterator.set(null)
        probe.request(1)
        probe.expectNext()
        probe.expectComplete()
        probe.cancel()
      }
    }

    "fail with error when GetStreamRequest fails" in assertAllStagesStopped {
      new KinesisSpecContext with WithGetShardIteratorSuccess with WithGetRecordsFailure {
        val probe = KinesisSource.basic(shardSettings, amazonKinesisAsync).runWith(TestSink.probe)
        probe.request(1)
        probe.expectError() shouldBe an[KinesisErrors.GetRecordsError]
        probe.cancel()
      }
    }
  }

  trait KinesisSpecContext {
    def shards: util.List[Shard] = util.Arrays.asList(Shard.builder().shardId("id").build())

    def shardIterator: String = "iterator"

    def records: util.List[Record] = new util.ArrayList()

    val nextShardIterator = new AtomicReference[String]("next")

    def describeStreamResult =
      DescribeStreamResponse
        .builder()
        .streamDescription(StreamDescription.builder().shards(shards).hasMoreShards(false).build())
        .build()

    when(amazonKinesisAsync.describeStream(any[DescribeStreamRequest]))
      .thenReturn(CompletableFuture.completedFuture(describeStreamResult))

    val getShardIteratorResult = GetShardIteratorResponse.builder().shardIterator(shardIterator).build()

    def getRecordsResult =
      GetRecordsResponse.builder().records(records).nextShardIterator(nextShardIterator.get()).build()

  }

  trait WithGetShardIteratorSuccess { self: KinesisSpecContext =>
    when(amazonKinesisAsync.getShardIterator(any[GetShardIteratorRequest])).thenAnswer(new Answer[AnyRef] {
      override def answer(invocation: InvocationOnMock): AnyRef =
        CompletableFuture.completedFuture(getShardIteratorResult)
    })
  }

  trait WithGetShardIteratorFailure { self: KinesisSpecContext =>
    when(amazonKinesisAsync.getShardIterator(any[GetShardIteratorRequest])).thenAnswer(new Answer[AnyRef] {
      override def answer(invocation: InvocationOnMock): AnyRef =
        CompletableFuture.completedFuture(getShardIteratorResult)
    })
  }

  trait WithGetRecordsSuccess { self: KinesisSpecContext =>
    when(amazonKinesisAsync.getRecords(any[GetRecordsRequest])).thenAnswer(new Answer[AnyRef] {
      override def answer(invocation: InvocationOnMock) =
        CompletableFuture.completedFuture(getRecordsResult)
    })
  }

  trait WithGetRecordsFailure { self: KinesisSpecContext =>
    when(amazonKinesisAsync.getRecords(any[GetRecordsRequest])).thenAnswer(new Answer[AnyRef] {
      override def answer(invocation: InvocationOnMock) = {
        val future = new CompletableFuture[GetRecordsResponse]()
        future.completeExceptionally(new Exception("fail"))
        future
      }
    })
  }
}
