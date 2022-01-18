/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import java.nio.ByteBuffer
import java.time.Instant
import java.util.concurrent.Semaphore

import akka.stream.KillSwitches
import akka.stream.alpakka.kinesis.CommittableRecord.{BatchData, ShardProcessorData}
import akka.stream.alpakka.kinesis.KinesisSchedulerErrors.SchedulerUnexpectedShutdown
import akka.stream.alpakka.kinesis.SwitchMode.{Close, Open}
import akka.stream.alpakka.kinesis.impl.ShardProcessor
import akka.stream.alpakka.kinesis.scaladsl.KinesisSchedulerSource
import akka.stream.alpakka.testkit.scaladsl.Repeated
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.model.Record
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.lifecycle.ShutdownReason
import software.amazon.kinesis.lifecycle.events.{InitializationInput, ProcessRecordsInput, ShardEndedInput}
import software.amazon.kinesis.processor.{
  RecordProcessorCheckpointer,
  ShardRecordProcessor,
  ShardRecordProcessorFactory
}
import software.amazon.kinesis.retrieval.KinesisClientRecord
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

import scala.jdk.CollectionConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

class KinesisSchedulerSourceSpec
    extends AnyWordSpec
    with Matchers
    with DefaultTestContext
    with Eventually
    with Repeated {

  "KinesisSchedulerSource" must {

    "publish records downstream" in assertAllStagesStopped(new KinesisSchedulerContext with TestData {
      val initializationInput: InitializationInput =
        randomInitializationInput()
      recordProcessor.initialize(initializationInput)
      recordProcessor.processRecords(sampleRecordsInput())

      val producedRecord: CommittableRecord = sinkProbe.requestNext()
      producedRecord.processorData.recordProcessorStartingSequenceNumber shouldBe initializationInput
        .extendedSequenceNumber()
      producedRecord.processorData.shardId shouldBe initializationInput.shardId()
      producedRecord.record shouldBe sampleRecord

      killSwitch.shutdown()

      sinkProbe.expectComplete()
    })

    "publish records downstream with backpressure" in assertAllStagesStopped(
      new KinesisSchedulerContext(bufferSize = 50, switchMode = Close) with TestData {
        recordProcessor.initialize(randomInitializationInput())

        private val allRecordsConsumedBySource = Future {
          for (_ <- 1 to 52) recordProcessor.processRecords(sampleRecordsInput())
        }

        Thread.sleep(100)

        // If the bufferSize is 50, only 51 records can be consumed by the Source
        // 50 -> The buffer of the main stage of the Source
        // 1 -> Other components of the Source take additional records, this number is affected by the config:
        //      akka.stream.materializer.initial-input-buffer-size
        //      akka.stream.materializer.max-input-buffer-size
        allRecordsConsumedBySource.isCompleted shouldBe false

        // We make room so more records are consumed/buffered along the stream
        valve.foreach(_.flip(Open))

        // All the 52 records can be consumed by the Source now
        eventually {
          allRecordsConsumedBySource.isCompleted shouldBe true
        }

        for (_ <- 1 to 52) sinkProbe.requestNext()
        killSwitch.shutdown()
        sinkProbe.expectComplete()
      }
    )

    "publish records downstream using different IRecordProcessor incarnations" in assertAllStagesStopped(
      new KinesisSchedulerContext with TestData {
        val initializationInput: InitializationInput =
          randomInitializationInput()
        recordProcessor.initialize(initializationInput)
        recordProcessor.processRecords(sampleRecordsInput())

        var producedRecord: CommittableRecord = sinkProbe.requestNext()
        producedRecord.processorData.recordProcessorStartingSequenceNumber shouldBe initializationInput
          .extendedSequenceNumber()
        producedRecord.processorData.shardId shouldBe initializationInput.shardId()
        producedRecord.record shouldBe sampleRecord

        val otherInitializationInput: InitializationInput =
          randomInitializationInput()
        otherRecordProcessor.initialize(otherInitializationInput)
        otherRecordProcessor.processRecords(sampleRecordsInput())

        producedRecord = sinkProbe.requestNext()
        producedRecord.processorData.recordProcessorStartingSequenceNumber shouldBe otherInitializationInput
          .extendedSequenceNumber()
        producedRecord.processorData.shardId shouldBe otherInitializationInput.shardId()
        producedRecord.record shouldBe sampleRecord

        killSwitch.shutdown()

        sinkProbe.expectComplete()
      }
    )

    "call Scheduler shutdown on stage completion" in assertAllStagesStopped(new KinesisSchedulerContext {
      killSwitch.shutdown()

      sinkProbe.expectComplete()
      eventually {
        verify(scheduler).run()
        verify(scheduler).shutdown()
      }
    })

    "complete the stage if the Scheduler is shutdown" in assertAllStagesStopped(new KinesisSchedulerContext {
      lock.release()
      sinkProbe.expectComplete()
      eventually {
        verify(scheduler).run()
      }
    })

    "complete the stage with error if the Scheduler fails" in assertAllStagesStopped(
      new KinesisSchedulerContext(
        Some(SchedulerUnexpectedShutdown(new RuntimeException()))
      ) {
        sinkProbe.expectError() shouldBe a[SchedulerUnexpectedShutdown]
        eventually {
          verify(scheduler).run()
        }
      }
    )

    "not drop messages in case of back-pressure" in assertAllStagesStopped(
      new KinesisSchedulerContext(bufferSize = 10, backpressureTimeout = FiniteDuration(1, SECONDS)) with TestData {
        recordProcessor.initialize(randomInitializationInput())
        Future {
          val records = (1 to 30).map { i =>
            val record = org.mockito.Mockito.mock(classOf[KinesisClientRecord])
            when(record.sequenceNumber).thenReturn(i.toString)
            record
          }
          recordProcessor.processRecords(sampleRecordsInput(records))
        }
        for (_ <- 1 to 30) {
          sinkProbe.requestNext()
          Thread.sleep(100)
        }

        killSwitch.shutdown()
        sinkProbe.expectComplete()
      }
    )

    "not drop messages in case of back-pressure with multiple shard schedulers" in assertAllStagesStopped(
      new KinesisSchedulerContext(
        bufferSize = 10,
        backpressureTimeout = FiniteDuration(2, SECONDS)
      ) with TestData {
        recordProcessor.initialize(randomInitializationInput())
        otherRecordProcessor.initialize(randomInitializationInput())

        def simulateSchedulerThread(rp: ShardRecordProcessor): Future[Unit] =
          Future {
            val records = (1 to 30).map { i =>
              val record = org.mockito.Mockito.mock(classOf[KinesisClientRecord])
              when(record.sequenceNumber).thenReturn(i.toString)
              record
            }
            rp.processRecords(sampleRecordsInput(records))
          }

        simulateSchedulerThread(recordProcessor)
        simulateSchedulerThread(otherRecordProcessor)

        for (_ <- 1 to 60) {
          sinkProbe.requestNext()
          Thread.sleep(100)
        }

        killSwitch.shutdown()
        sinkProbe.expectComplete()
      }
    )

    "not drop messages in case of Shard end" in assertAllStagesStopped(
      new KinesisSchedulerContext(bufferSize = 10) with TestData {
        recordProcessor.initialize(randomInitializationInput())
        val shardEndedCheckpointer: RecordProcessorCheckpointer =
          org.mockito.Mockito.mock(classOf[software.amazon.kinesis.processor.RecordProcessorCheckpointer])

        val shardEndedCallFinished = for {
          _ <- Future {
            val records: Seq[KinesisClientRecord] = (1 to 30).map { i =>
              val record = org.mockito.Mockito.mock(classOf[KinesisClientRecord])
              when(record.sequenceNumber).thenReturn(i.toString)
              record
            }
            recordProcessor.processRecords(sampleRecordsInput(records, isShardEnd = true))
          }
          _ <- Future {
            val shardEndedInput = org.mockito.Mockito.mock(classOf[ShardEndedInput])
            when(shardEndedInput.checkpointer()).thenReturn(shardEndedCheckpointer)
            recordProcessor.shardEnded(shardEndedInput)
          }
        } yield ()

        verify(shardEndedCheckpointer, never()).checkpoint()

        for (_ <- 1 to 29) sinkProbe.requestNext()

        verify(shardEndedCheckpointer, never()).checkpoint()

        sinkProbe.requestNext().tryToCheckpoint()

        eventually {
          verify(shardEndedCheckpointer).checkpoint()
          shardEndedCallFinished.isCompleted shouldBe true
        }

        killSwitch.shutdown()
        sinkProbe.expectComplete()
      }
    )
  }

  private abstract class KinesisSchedulerContext(schedulerFailure: Option[Throwable] = None,
                                                 bufferSize: Int = 100,
                                                 backpressureTimeout: FiniteDuration = 1.minute,
                                                 switchMode: SwitchMode = Open) {
    val scheduler: Scheduler = org.mockito.Mockito.mock(classOf[Scheduler])

    val lock = new Semaphore(0)
    when(scheduler.run()).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit =
        schedulerFailure.fold(lock.acquire())(throw _)
    })

    private val semaphore = new Semaphore(0)

    var recordProcessor: ShardRecordProcessor = _
    var otherRecordProcessor: ShardRecordProcessor = _
    private val schedulerBuilder = { x: ShardRecordProcessorFactory =>
      recordProcessor = x.shardRecordProcessor()
      otherRecordProcessor = x.shardRecordProcessor()
      semaphore.release()
      scheduler
    }
    val (((valve, killSwitch), watch), sinkProbe) =
      KinesisSchedulerSource(schedulerBuilder,
                             KinesisSchedulerSourceSettings(bufferSize = bufferSize,
                                                            backpressureTimeout = backpressureTimeout))
        .viaMat(Valve(switchMode))(Keep.right)
        .viaMat(KillSwitches.single)(Keep.both)
        .watchTermination()(Keep.both)
        .toMat(TestSink.probe)(Keep.both)
        .run()

    watch.onComplete(_ => lock.release())

    sinkProbe.ensureSubscription()
    sinkProbe.request(1)

    semaphore.acquire()
  }

  private trait TestData {
    protected val checkpointer: RecordProcessorCheckpointer =
      org.mockito.Mockito.mock(classOf[RecordProcessorCheckpointer])

    def randomInitializationInput(): InitializationInput =
      InitializationInput
        .builder()
        .shardId(Random.nextString(10))
        .extendedSequenceNumber(new ExtendedSequenceNumber(Random.nextString(10), Random.nextLong()))
        .build()

    val sampleRecord: KinesisClientRecord =
      KinesisClientRecord.fromRecord(
        Record
          .builder()
          .approximateArrivalTimestamp(Instant.now())
          .encryptionType("encryption")
          .partitionKey("partitionKey")
          .sequenceNumber("sequenceNum")
          .data(SdkBytes.fromByteBuffer(ByteBuffer.wrap(Array[Byte](1))))
          .build()
      )

    def sampleRecordsInput(records: Seq[KinesisClientRecord] = sampleRecord :: Nil,
                           isShardEnd: Boolean = false): ProcessRecordsInput =
      ProcessRecordsInput
        .builder()
        .checkpointer(checkpointer)
        .millisBehindLatest(1L)
        .records(records.asJava)
        .isAtShardEnd(isShardEnd)
        .build()
  }

  "KinesisSchedulerSource checkpoint Flow " must {

    "checkpoint batch of records with same sequence number" in new KinesisSchedulerCheckpointContext {
      val checkpointer: KinesisClientRecord => Unit =
        org.mockito.Mockito.mock(classOf[KinesisClientRecord => Unit])
      var latestRecord: KinesisClientRecord = _
      val allRecordsPushed: Future[Unit] = Future {
        for (i <- 1 to 3) {
          val record = org.mockito.Mockito.mock(classOf[KinesisClientRecord])
          when(record.sequenceNumber).thenReturn("1")
          when(record.subSequenceNumber).thenReturn(i.toLong)
          sourceProbe.sendNext(
            new CommittableRecord(
              record,
              new BatchData(null, null, false, 0),
              new ShardProcessorData(
                "shard-1",
                null,
                null
              )
            ) {
              override def shutdownReason: Option[ShutdownReason] = None

              override def forceCheckpoint(): Unit = checkpointer(record)
            }
          )
          latestRecord = record
        }
      }

      for (_ <- 1 to 3) sinkProbe.requestNext()

      eventually {
        allRecordsPushed.isCompleted shouldBe true
        verify(checkpointer).apply(latestRecord)
      }

      sourceProbe.sendComplete()
      sinkProbe.expectComplete()
    }

    "checkpoint batch of records of different shards" in new KinesisSchedulerCheckpointContext {
      val checkpointerShard1: KinesisClientRecord => Unit =
        org.mockito.Mockito.mock(classOf[KinesisClientRecord => Unit])
      var latestRecordShard1: KinesisClientRecord = _
      var latestRecordShard2: KinesisClientRecord = _
      val checkpointerShard2: KinesisClientRecord => Unit =
        org.mockito.Mockito.mock(classOf[KinesisClientRecord => Unit])

      val allRecordsPushed: Future[Unit] = Future {
        for (i <- 1 to 3) {
          val record = org.mockito.Mockito.mock(classOf[KinesisClientRecord])
          when(record.sequenceNumber).thenReturn(i.toString)
          sourceProbe.sendNext(
            new CommittableRecord(
              record,
              new BatchData(null, null, false, 0),
              new ShardProcessorData(
                "shard-1",
                null,
                null
              )
            ) {
              override def shutdownReason: Option[ShutdownReason] = None

              override def forceCheckpoint(): Unit = checkpointerShard1(record)
            }
          )
          latestRecordShard1 = record
        }
        for (i <- 1 to 3) {
          val record = org.mockito.Mockito.mock(classOf[KinesisClientRecord])
          when(record.sequenceNumber).thenReturn(i.toString)
          sourceProbe.sendNext(
            new CommittableRecord(
              record,
              new BatchData(null, null, false, 0),
              new ShardProcessorData(
                "shard-2",
                null,
                null
              )
            ) {
              override def shutdownReason: Option[ShutdownReason] = None

              override def forceCheckpoint(): Unit = checkpointerShard2(record)
            }
          )
          latestRecordShard2 = record
        }
      }

      for (_ <- 1 to 6) sinkProbe.requestNext()

      eventually {
        allRecordsPushed.isCompleted shouldBe true
        verify(checkpointerShard1).apply(latestRecordShard1)
        verify(checkpointerShard2).apply(latestRecordShard2)
      }

      sourceProbe.sendComplete()
      sinkProbe.expectComplete()
    }

    "fail with Exception if checkpoint action fails" in new KinesisSchedulerCheckpointContext {
      val record: KinesisClientRecord = org.mockito.Mockito.mock(classOf[KinesisClientRecord])
      when(record.sequenceNumber).thenReturn("1")
      val checkpointer: KinesisClientRecord => Unit =
        org.mockito.Mockito.mock(classOf[KinesisClientRecord => Unit])
      val committableRecord: CommittableRecord = new CommittableRecord(
        record,
        new BatchData(null, null, false, 0),
        new ShardProcessorData(
          "shard-1",
          null,
          null
        )
      ) {
        override def shutdownReason: Option[ShutdownReason] = None
        override def forceCheckpoint(): Unit = checkpointer(record)
      }
      sourceProbe.sendNext(committableRecord)

      val failure = new RuntimeException()
      when(checkpointer.apply(record)).thenThrow(failure)

      sinkProbe.request(1)

      sinkProbe.expectError(failure)
    }
  }

  private trait KinesisSchedulerCheckpointContext {
    val (sourceProbe, sinkProbe) =
      TestSource
        .probe[CommittableRecord]
        .via(
          KinesisSchedulerSource
            .checkpointRecordsFlow(
              KinesisSchedulerCheckpointSettings(maxBatchSize = 100, maxBatchWait = 500.millis)
            )
        )
        .toMat(TestSink.probe)(Keep.both)
        .run()
    val recordProcessor = new ShardProcessor(_ => ())
  }

}
