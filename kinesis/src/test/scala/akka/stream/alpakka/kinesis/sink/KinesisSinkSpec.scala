package akka.stream.alpakka.kinesis.sink

import akka.actor.ActorSystem
import akka.stream.ThrottleMode
import akka.stream.alpakka.kinesis.KinesisLimits._
import akka.stream.alpakka.kinesis.KinesisMetrics._
import akka.stream.alpakka.kinesis.TestKinesisMetrics
import akka.stream.alpakka.kinesis.sink.KinesisSink.{Failed, PutRecords, Retryable, Succeeded}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.testkit.TestKit
import org.scalatest.Assertions
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike

import java.math.BigInteger
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}

class KinesisSinkSpec extends TestKit(ActorSystem("KinesisSinkTest")) with AsyncWordSpecLike with Matchers {

  private val defaultSettings = KinesisSinkSettings(
    streamName = "testStream",
    nrOfInstance = 1,
    maxAggRecordBytes = 4 << 10,
    maxBatchRequestBytes = 20 << 10,
    maxConcurrentRequests = 8,
    maxBufferedBytes = Some(2 << 20),
    maxBufferedDuration = None,
    maxBurstDuration = 100.millis
  )

  private def testSharding(numShards: Int) = new Sharding {
    override def nrOfShards: Int = numShards
    override def getShard(hashKey: BigInteger): Int = (BigInt(hashKey) % nrOfShards).toInt
  }

  private def testKinesisSink[In: Aggregatable](
      source: Source[In, _],
      putRecords: PutRecords,
      settings: KinesisSinkSettings = defaultSettings,
      aggregatorFactory: Aggregator.Factory[In] = Aggregator.default[In],
      bufferFactory: Buffer.Factory = Buffer.dropHead,
      nrOfShards: Int = 1,
      nrOfInstances: Int = 1
  )(implicit metrics: TestKinesisMetrics = new TestKinesisMetrics(nrOfShards)) = {

    val kinesisSink =
      KinesisSink(settings, aggregatorFactory, bufferFactory, testSharding(nrOfShards), putRecords)

    val streamDone = Future
      .sequence((0 until nrOfInstances).map(_ => source.runWith(kinesisSink)))
      .map(_ => metrics)

    Source
      .repeat(1)
      .merge(Source.future(streamDone), eagerComplete = true)
      .throttle(1, 1.second, 1, ThrottleMode.Shaping)
      .map(_ => metrics.sample())
      .alsoTo(Flow[metrics.Sample].sliding(2).to(Sink.foreach(s => s.last.diff(s.head).print())))
      .runWith(Sink.seq)
      .andThen {
        case util.Success(s) =>
          printf("=== Final ===\n")
          s.last.diff(s.head).print()
      }
  }

  private def testSource(bytesPerRecord: Int, bytesPerSecond: Int, duration: FiniteDuration, nrOfShards: Int = 1) = {
    val key = "key"
    val data = Array.fill(bytesPerRecord - key.length)(0: Byte)
    Source
      .fromIterator(() => Iterator.from(0))
      .map(i => (key, BigInteger.valueOf(i % nrOfShards), data))
      .throttle(bytesPerSecond, 1.second, _ => bytesPerRecord)
      .take(duration.toSeconds * bytesPerSecond / bytesPerRecord)
  }

  private def testPutFlow(requestsPerSecond: Int) =
    Flow[Iterable[Aggregated]]
      .throttle(requestsPerSecond, 1.second, 0, ThrottleMode.Shaping)
      .mapConcat(identity)
      .map(Succeeded)

  "AggGroup" should {
    "be compared by key" in {
      val a: AggGroup = new AggGroup {
        override def key: AnyRef = "a"
      }
      val b = AggGroup("a")
      a shouldEqual b
    }
  }

  "KinesisSink" should {
    "drop invalid record" in {
      testKinesisSink(
        source = Source.repeat(("key", Array.fill(1 << 20)(0: Byte))).take(10),
        putRecords = Flow[Iterable[Aggregated]].mapConcat(identity).map(Succeeded)
      ).map { samples =>
        val progress = samples.last.diff(samples.head)
        progress.countOf(UserRecordTooLarge) shouldBe 10
        progress.countOf(UserRecordSuccess) shouldBe 0
        progress.countOf(UserRecordFailure) shouldBe 0
        progress.countOf(UserRecordDropped) shouldBe 0
        progress.countOf(AggRecordRetryable) shouldBe 0
      }
    }

    "write data with put records" in {
      testKinesisSink(
        source = Source.repeat(("key", "value".getBytes())).take(100),
        putRecords = Flow[Iterable[Aggregated]].mapConcat(identity).map(Succeeded)
      ).map { samples =>
        val progress = samples.last.diff(samples.head)
        progress.countOf(UserRecordSuccess) shouldBe 100
      }
    }

    "handle retryable failures" in {
      val record = ("key", "value".getBytes())
      testKinesisSink(
        source = Source
          .fromIterator(() => Iterator.fill(19)(record))
          .merge(Source.single(record).initialDelay(500.millis)),
        putRecords = Flow[Iterable[Aggregated]].mapConcat(identity).zipWithIndex.map {
          case (request, index) if index < 5 => Retryable(request)
          case (request, _) => Succeeded(request)
        }
      ).map { samples =>
        val progress = samples.last.diff(samples.head)
        progress.countOf(UserRecordDropped) shouldBe 0
        progress.countOf(UserRecordSuccess) shouldBe 20
        progress.countOf(AggRecordRetryable) shouldBe 5
      }
    }

    "buffer records" in {
      testKinesisSink(
        settings = defaultSettings,
        source = Source.repeat(("key", Array.fill(1024)(0: Byte))).take(1024),
        putRecords = Flow[Iterable[Aggregated]].mapConcat(identity).initialDelay(500.millis).map(Succeeded)
      ).map { samples =>
        val progress = samples.last.diff(samples.head)
        progress.print()
        progress.countOf(UserRecordSuccess) shouldBe 1024
        progress.valueOf(BufferedPayloadBytes) shouldBe 0
        progress.valueOf(AggregationGroups) shouldBe 0
      }
    }

    "throttle by number of records" in {
      testKinesisSink(
        settings = defaultSettings.copy(maxBufferedBytes = Some(1024), maxAggRecordBytes = 1),
        aggregatorFactory = Aggregator.default,
        bufferFactory = Buffer.backpressure,
        source = testSource(bytesPerRecord = 10, bytesPerSecond = 50000, duration = 1.second),
        putRecords = Flow[Iterable[Aggregated]].mapConcat(identity).map(Succeeded)
      ).map { samples =>
        val progress = samples.last.diff(samples.head)
        progress.elapsed should be > 5f
        progress.meanRate(AggRecordSuccess) should be < 1000f
      }
    }

    "throttle by throughput" in {
      testKinesisSink(
        settings = defaultSettings.copy(maxAggRecordBytes = 1 << 19, maxBufferedBytes = Some(2 << 20)),
        bufferFactory = Buffer.dropHead,
        source = testSource(bytesPerRecord = 1024, bytesPerSecond = 2 << 20, duration = 5.second),
        putRecords = Flow[Iterable[Aggregated]].mapConcat(identity).map(Succeeded)
      ).map { samples =>
        val progress = samples.last.diff(samples.head)
        progress.elapsed should be > 5f
        progress.meanRate(BytesSent) should be < (1 << 20).toFloat
      }
    }

    "count records properly" in {
      val (totalRecords, source) =
        testSource(bytesPerRecord = 64, bytesPerSecond = MaxBytesPerSecondPerShard * 120 / 100, duration = 5.second)
          .wireTapMat(Sink.fold(0)((cnt, _) => cnt + 1))(Keep.right)
          .preMaterialize()
      testKinesisSink(
        settings = defaultSettings.copy(maxBufferedBytes = Some(1024), maxAggRecordBytes = 1),
        source = source,
        bufferFactory = Buffer.dropHead,
        aggregatorFactory = Aggregator.default,
        putRecords = Flow[Iterable[Aggregated]].mapConcat(identity).zipWithIndex.map {
          case (request, index) if index < 100 => Retryable(request)
          case (request, index) if index < 200 => Failed(request)
          case (request, _) => Succeeded(request)
        }
      ).flatMap { samples =>
          val progress = samples.last.diff(samples.head)
          totalRecords.map((progress, _))
        }
        .map {
          case (metrics, totalRecords) =>
            val retryable = metrics.countOf(AggRecordRetryable)
            val success = metrics.countOf(UserRecordSuccess)
            val failure = metrics.countOf(UserRecordFailure)
            val dropped = metrics.countOf(UserRecordDropped)
            retryable shouldBe 100L
            failure shouldBe 100L
            dropped should be > 0L
            (success + failure + dropped) shouldBe totalRecords
        }
    }

    "drop on close" in {
      testKinesisSink(
        bufferFactory = Buffer.backpressure,
        source = Source.repeat(("key", "value".getBytes)).take(100),
        putRecords = Flow[Iterable[Aggregated]].mapConcat(identity).map(Retryable)
      ).map { samples =>
        val progress = samples.last.diff(samples.head)
        Thread.sleep(1000)
        progress.countOf(UserRecordDropped) shouldBe 100
      }
    }

    "aggregate small records to maximize throughput" in {
      val requestsPerSecond = 50
      val settings = defaultSettings.copy(
        maxAggRecordBytes = MaxBytesPerRecord / requestsPerSecond * 2,
        maxBatchRequestBytes = 1 // disable batching to test aggregation only
      )
      testKinesisSink(
        settings = settings,
        source = testSource(bytesPerRecord = 512, bytesPerSecond = MaxBytesPerSecondPerShard, duration = 5.seconds),
        bufferFactory = Buffer.dropHead,
        putRecords = testPutFlow(requestsPerSecond)
      ).map { samples =>
          val midSamples = samples.drop(2).dropRight(2)
          val progress = midSamples.last.diff(midSamples.head)
          progress.meanRate(BytesSent) should be > MaxBytesPerSecondPerShard * 0.9f
        }
    }

    "maximize throughput with limited number of batch requests" in {
      val requestsPerSecond = 24
      val settings = defaultSettings.copy(
        maxBatchRequestBytes = MaxBytesPerSecondPerShard / requestsPerSecond,
        maxAggRecordBytes = 1 // disable aggregation to test batching only
      )
      testKinesisSink(
        settings = settings,
        aggregatorFactory = Aggregator.default,
        source = testSource(bytesPerRecord = 1024, bytesPerSecond = MaxBytesPerSecondPerShard, duration = 5.seconds),
        bufferFactory = Buffer.backpressure,
        putRecords = testPutFlow(requestsPerSecond)
      ).map { samples =>
          val midSamples = samples.drop(2).dropRight(2)
          val progress = midSamples.last.diff(midSamples.head)
          progress.meanRate(BytesSent) should be > MaxBytesPerSecondPerShard * 0.9f
        }
    }

    "aggregate by group" in {
      val nrOfRecords = 1024
      val nrOfAggGroups = 64
      testKinesisSink(
        settings = defaultSettings.copy(maxAggRecordBytes = MaxBytesPerRecord, maxBatchRequestBytes = 1),
        source = Source(0 until nrOfRecords).map(i => (i.toString, Array.fill(1)(0: Byte))),
        aggregatorFactory = Aggregator.by[(String, Array[Byte])](in => AggGroup(in._1.toInt % nrOfAggGroups)),
        bufferFactory = Buffer.backpressure,
        putRecords = Flow[Iterable[Aggregated]].initialDelay(100.millis).mapConcat(identity).map(Succeeded)
      ).map { samples =>
          val progress = samples.last.diff(samples.head)
          progress.countOf(AggRecordSuccess).toInt should be > nrOfAggGroups
          progress.countOf(UserRecordSuccess) shouldBe nrOfRecords
        }
    }

    "write to multiple shards" in {
      val nrOfShards = 4
      val bytesPerSecond = MaxBytesPerSecondPerShard * nrOfShards

      testKinesisSink(
        settings = defaultSettings.copy(maxAggRecordBytes = MaxBytesPerRecord, maxBatchRequestBytes = 1),
        source = testSource(
          bytesPerRecord = 128,
          bytesPerSecond = bytesPerSecond,
          duration = 5.second,
          nrOfShards = nrOfShards
        ),
        bufferFactory = Buffer.backpressure,
        nrOfShards = nrOfShards,
        putRecords = Flow[Iterable[Aggregated]].mapConcat(identity).map(Succeeded)
      ).map { samples =>
          val midSamples = samples.drop(2).dropRight(2)
          val progress = midSamples.last.diff(midSamples.head)
          progress.meanRate(BytesSuccess) should be > bytesPerSecond * 0.9f
        }
    }

    "avoid data congestion by throtting partitions individually" in {
      val nrOfShards = 2
      val nrOfInstance = 4
      val testDuration = 10.seconds
      val payloadSize = 1024
      val bandwidth = MaxBytesPerSecondPerShard * nrOfShards * 100 / 100
      val maxConcurrentRequests = 16

      val putRecords = KinesisSimulation.putRecords(nrOfShards)

      def runTest(throttlePerShard: Boolean) = {
        testKinesisSink(
          settings = defaultSettings.copy(
            nrOfInstance = nrOfInstance,
            maxAggRecordBytes = 4 << 10,
            maxBatchRequestBytes = 20 << 10,
            maxBurstDuration = 100.millis
          ),
          source = testSource(payloadSize, bandwidth / nrOfInstance, testDuration, nrOfShards),
          bufferFactory = Buffer.dropHead,
          nrOfShards = nrOfShards,
          nrOfInstances = nrOfInstance,
          putRecords =
            Flow[Iterable[Aggregated]].mapAsyncUnordered(maxConcurrentRequests)(putRecords).mapConcat(identity)
        )
      }

      for {
        _ <- runTest(throttlePerShard = true).map { samples =>
          val midSamples = samples.drop(2).dropRight(2)
          val progress = midSamples.last.diff(midSamples.head)
          val rateOfRetryable = progress.meanRate(BytesRetryable)
          val rateOfSuccess = progress.meanRate(BytesSuccess)
          rateOfRetryable / (rateOfRetryable + rateOfSuccess) should be < 0.05f
        }
        _ <- runTest(throttlePerShard = false).map { samples =>
          val midSamples = samples.drop(2).dropRight(2)
          val progress = midSamples.last.diff(midSamples.head)
          val rateOfRetryable = progress.meanRate(BytesRetryable)
          val rateOfSuccess = progress.meanRate(BytesSuccess)
          rateOfRetryable / (rateOfRetryable + rateOfSuccess) should be > 0.4f
        }
      } yield Assertions.succeed
    }
  }

  "IgnoreDownstreamClose" should {
    "ignore downstream complete" in {
      val dropped = new AtomicInteger(0)
      Source(1 to 100)
        .alsoTo(Flow[Int].via(new DropOnClose(_ => dropped.incrementAndGet())).take(0).to(Sink.ignore))
        .runWith(Sink.ignore)
        .map(_ => dropped.get() shouldBe 100)
    }

    "ignore downstream failure" in {
      val dropped = new AtomicInteger(0)
      Source(1 to 100)
        .alsoTo(
          Flow[Int]
            .via(new DropOnClose(_ => dropped.incrementAndGet()))
            .to(Sink.foreach(_ => throw new RuntimeException))
        )
        .runWith(Sink.ignore)
        .map(_ => dropped.get() shouldBe 99)
    }
  }
}
