/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.kinesis.impl

import java.util.Collections

import akka.stream.alpakka.kinesis.{CommittableRecord, DefaultTestContext}
import org.mockito.Mockito.{mock => jMock, verify, when}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import software.amazon.kinesis.lifecycle.events.{InitializationInput, ProcessRecordsInput, ShardEndedInput}
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import software.amazon.kinesis.retrieval.KinesisClientRecord

class ShardProcessorSpec extends AnyWordSpec with Matchers with DefaultTestContext {

  private def mock[T](implicit ct: scala.reflect.ClassTag[T]): T =
    jMock(ct.runtimeClass.asInstanceOf[Class[T]])

  private def mockKinesisRecord(sequenceNumber: String, subSequenceNumber: Long): KinesisClientRecord = {
    val r = mock[KinesisClientRecord]
    when(r.sequenceNumber()).thenReturn(sequenceNumber)
    when(r.subSequenceNumber()).thenReturn(subSequenceNumber)
    r
  }

  private def mockInitInput(shardId: String): InitializationInput = {
    val input = mock[InitializationInput]
    when(input.shardId()).thenReturn(shardId)
    when(input.extendedSequenceNumber()).thenReturn(null)
    when(input.pendingCheckpointSequenceNumber()).thenReturn(null)
    input
  }

  private def mockProcessRecordsInput(
      records: java.util.List[KinesisClientRecord],
      checkpointer: RecordProcessorCheckpointer,
      isAtShardEnd: Boolean
  ): ProcessRecordsInput = {
    val input = mock[ProcessRecordsInput]
    when(input.records()).thenReturn(records)
    when(input.checkpointer()).thenReturn(checkpointer)
    when(input.cacheEntryTime()).thenReturn(java.time.Instant.now())
    when(input.cacheExitTime()).thenReturn(java.time.Instant.now())
    when(input.isAtShardEnd()).thenReturn(isAtShardEnd)
    when(input.millisBehindLatest()).thenReturn(0L)
    input
  }

  private def mockShardEndedInput(checkpointer: RecordProcessorCheckpointer): ShardEndedInput = {
    val input = mock[ShardEndedInput]
    when(input.checkpointer()).thenReturn(checkpointer)
    input
  }

  "ShardProcessor" should {

    "block shardEnded until the last record in the batch is checkpointed" in {
      val records = Collections.singletonList(mockKinesisRecord("seq-1", 0L))
      val checkpointer = mock[RecordProcessorCheckpointer]
      var capturedRecord: Option[CommittableRecord] = None

      val processor = new ShardProcessor(r => capturedRecord = Some(r))
      processor.initialize(mockInitInput("shard-1"))
      processor.processRecords(mockProcessRecordsInput(records, checkpointer, isAtShardEnd = true))

      val shardEndedDone = new java.util.concurrent.atomic.AtomicBoolean(false)
      val shardEndedThread = new Thread(
        () => {
          processor.shardEnded(mockShardEndedInput(checkpointer))
          shardEndedDone.set(true)
        }
      )
      shardEndedThread.start()

      // shardEnded should block because the record has not been checkpointed yet
      Thread.sleep(100)
      shardEndedDone.get() shouldBe false

      // checkpoint the last record; this should release the semaphore and let shardEnded proceed
      capturedRecord.get.forceCheckpoint()
      shardEndedThread.join(2000)
      shardEndedDone.get() shouldBe true

      verify(checkpointer).checkpoint("seq-1", 0L)
      verify(checkpointer).checkpoint()
    }

    "when the last batch is empty, block shardEnded until the previous batch's last record is checkpointed" in {
      val checkpointer = mock[RecordProcessorCheckpointer]
      var lastSeenRecord: Option[CommittableRecord] = None

      val processor = new ShardProcessor(r => lastSeenRecord = Some(r))
      processor.initialize(mockInitInput("shard-1"))

      // First batch: one record, not at shard end (simulates that the final batch will be empty)
      val firstBatchRecords = Collections.singletonList(mockKinesisRecord("seq-1", 0L))
      processor.processRecords(mockProcessRecordsInput(firstBatchRecords, checkpointer, isAtShardEnd = false))
      val recordFromFirstBatch = lastSeenRecord.get

      // Second batch: empty, at shard end (KCL can call processRecords with 0 records)
      processor.processRecords(mockProcessRecordsInput(Collections.emptyList(), checkpointer, isAtShardEnd = true))

      val shardEndedDone = new java.util.concurrent.atomic.AtomicBoolean(false)
      val shardEndedThread = new Thread(
        () => {
          processor.shardEnded(mockShardEndedInput(checkpointer))
          shardEndedDone.set(true)
        }
      )
      shardEndedThread.start()

      // shardEnded must wait for the previous batch's last record, not complete immediately
      Thread.sleep(100)
      shardEndedDone.get() shouldBe false

      recordFromFirstBatch.forceCheckpoint()
      shardEndedThread.join(2000)
      shardEndedDone.get() shouldBe true

      verify(checkpointer).checkpoint("seq-1", 0L)
      verify(checkpointer).checkpoint()
    }

    "complete shardEnded immediately when no records were ever processed" in {
      val checkpointer = mock[RecordProcessorCheckpointer]
      val processor = new ShardProcessor(_ => ())

      processor.initialize(mockInitInput("shard-1"))
      // processRecords never called with any records (or only empty batches)
      processor.processRecords(mockProcessRecordsInput(Collections.emptyList(), checkpointer, isAtShardEnd = true))

      processor.shardEnded(mockShardEndedInput(checkpointer))

      verify(checkpointer).checkpoint()
    }
  }
}
