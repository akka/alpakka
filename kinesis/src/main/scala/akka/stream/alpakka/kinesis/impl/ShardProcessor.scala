/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.kinesis.impl

import java.util.concurrent.Semaphore

import akka.annotation.InternalApi
import akka.stream.alpakka.kinesis.CommittableRecord
import akka.stream.alpakka.kinesis.CommittableRecord.{BatchData, ShardProcessorData}
import software.amazon.kinesis.lifecycle.ShutdownReason
import software.amazon.kinesis.lifecycle.events._
import software.amazon.kinesis.processor.{RecordProcessorCheckpointer, ShardRecordProcessor}
import software.amazon.kinesis.retrieval.KinesisClientRecord

import scala.jdk.CollectionConverters._

@InternalApi
private[kinesis] class ShardProcessor(
    processRecord: CommittableRecord => Unit
) extends ShardRecordProcessor {

  // Coordination for shard end: we must not call checkpointer.checkpoint() in shardEnded until
  // the last record we handed out has been checkpointed. Otherwise, when the final batch has
  // no records (isAtShardEnd was never true in the previous batch), shardEnded would complete
  // immediately and later checkpoint attempts for in-flight records would fail because the
  // shard is already checkpointed with SHARD_END. We tie a semaphore to the last record of
  // each batch; that record releases it on checkpoint. shardEnded waits on it before
  // checkpointing the shard. If the last batch is empty, lastRecordSemaphore still refers to
  // the previous batch's last record, so we correctly wait for it.
  @volatile
  private var lastRecordSemaphore: Option[Semaphore] = None

  private var shardData: ShardProcessorData = _
  private var checkpointer: RecordProcessorCheckpointer = _
  private var shutdown: Option[ShutdownReason] = None

  override def initialize(initializationInput: InitializationInput): Unit =
    shardData = new ShardProcessorData(initializationInput.shardId,
                                       initializationInput.extendedSequenceNumber,
                                       initializationInput.pendingCheckpointSequenceNumber)

  override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
    checkpointer = processRecordsInput.checkpointer()

    val batchData = new BatchData(processRecordsInput.cacheEntryTime,
                                  processRecordsInput.cacheExitTime,
                                  processRecordsInput.isAtShardEnd,
                                  processRecordsInput.millisBehindLatest)

    val numberOfRecords = processRecordsInput.records().size()
    processRecordsInput.records().asScala.zipWithIndex.foreach {
      case (record, index) =>
        // Only the last record in the batch gets a semaphore; when it is checkpointed it
        // releases it so shardEnded can proceed. Non-last records pass None.
        val committableRecord =
          if (index + 1 == numberOfRecords) {
            lastRecordSemaphore = Some(new Semaphore(0))
            new InternalCommittableRecord(record, batchData, checkpointed = lastRecordSemaphore)
          } else {
            new InternalCommittableRecord(record, batchData, None)
          }
        processRecord(committableRecord)
    }
  }

  override def leaseLost(leaseLostInput: LeaseLostInput): Unit =
    // We cannot checkpoint at this point as we don't have the
    // lease anymore
    shutdown = Some(ShutdownReason.LEASE_LOST)

  override def shardEnded(shardEndedInput: ShardEndedInput): Unit = {
    checkpointer = shardEndedInput.checkpointer()
    // We must checkpoint to finish the shard, but we wait until the last record we handed
    // out has been checkpointed (releases the semaphore). If no records were ever created,
    // lastRecordSemaphore is None and we proceed without waiting.
    shutdown = Some(ShutdownReason.SHARD_END)
    lastRecordSemaphore.foreach(_.acquire())
    checkpointer.checkpoint()
  }

  override def shutdownRequested(shutdownInput: ShutdownRequestedInput): Unit = {
    checkpointer = shutdownInput.checkpointer()
    // We don't checkpoint at this point as we assume the
    // standard mechanism will checkpoint when required
    shutdown = Some(ShutdownReason.REQUESTED)
  }

  final class InternalCommittableRecord(
      record: KinesisClientRecord,
      batchData: BatchData,
      checkpointed: Option[Semaphore]
  ) extends CommittableRecord(record, batchData, shardData) {
    private def checkpoint(): Unit = {
      checkpointer.checkpoint(sequenceNumber, subSequenceNumber)
      // Signal shardEnded that this (last) record is done so it can safely checkpoint the shard.
      checkpointed.foreach(_.release())
    }
    override def shutdownReason: Option[ShutdownReason] = shutdown
    override def forceCheckpoint(): Unit =
      checkpoint()
  }
}
