/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
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

import scala.collection.JavaConverters._

@InternalApi
private[kinesis] class ShardProcessor(
    processRecord: CommittableRecord => Unit
) extends ShardRecordProcessor {

  // We need extra coordination in the event of a Shard End (for example, when we double
  // the number of shards, the old shards are ended and the new shards take their place).
  //
  // When a shard needs to be ended, the "shardEnded" method is invoked. We block that
  // invocation until all records (actually we only care about the latest record)
  // are checkpointed using the asynchronous stream mechanism.
  //
  // When all records are checkpointed, the "shardEnded" method can safely acknowledge
  // that the shard can be ended.
  //
  // If we were not to do this extra work, the shard would be ended before we made sure
  // that all the records have been successfully consumed by the stream.
  //
  // To do the coordination we use a Semaphore instance in every ShardProcessor
  // instance. Both the ShardProcessor and its Semaphore will live in a thread spawned
  // by the AWS KCL Scheduler after the Scheduler run() method has been invoked.
  private val lastRecordSemaphore = new Semaphore(1)

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

    if (batchData.isAtShardEnd) {
      lastRecordSemaphore.acquire()
    }

    val numberOfRecords = processRecordsInput.records().size()
    processRecordsInput.records().asScala.zipWithIndex.foreach {
      case (record, index) =>
        processRecord(
          new InternalCommittableRecord(
            record,
            batchData,
            lastRecord = processRecordsInput.isAtShardEnd && index + 1 == numberOfRecords
          )
        )
    }
  }

  override def leaseLost(leaseLostInput: LeaseLostInput): Unit =
    // We cannot checkpoint at this point as we don't have the
    // lease anymore
    shutdown = Some(ShutdownReason.LEASE_LOST)

  override def shardEnded(shardEndedInput: ShardEndedInput): Unit = {
    checkpointer = shardEndedInput.checkpointer()
    // We must checkpoint to finish the shard, but we wait
    // until all records in flight have been processed
    shutdown = Some(ShutdownReason.SHARD_END)
    lastRecordSemaphore.acquire()
    checkpointer.checkpoint()
  }

  override def shutdownRequested(shutdownInput: ShutdownRequestedInput): Unit = {
    checkpointer = shutdownInput.checkpointer()
    // We don't checkpoint at this point as we assume the
    // standard mechanism will checkpoint when required
    shutdown = Some(ShutdownReason.REQUESTED)
  }

  final class InternalCommittableRecord(record: KinesisClientRecord, batchData: BatchData, lastRecord: Boolean)
      extends CommittableRecord(record, batchData, shardData) {
    private def checkpoint(): Unit = {
      checkpointer.checkpoint(sequenceNumber, subSequenceNumber)
      if (lastRecord) lastRecordSemaphore.release()
    }
    override def shutdownReason: Option[ShutdownReason] = shutdown
    override def forceCheckpoint(): Unit =
      checkpoint()
  }
}
