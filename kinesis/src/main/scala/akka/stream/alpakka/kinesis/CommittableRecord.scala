/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import java.time.Instant

import akka.annotation.InternalApi
import akka.stream.alpakka.kinesis.CommittableRecord.{BatchData, ShardProcessorData}
import software.amazon.kinesis.exceptions.ShutdownException
import software.amazon.kinesis.lifecycle.ShutdownReason
import software.amazon.kinesis.retrieval.KinesisClientRecord
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

abstract class CommittableRecord @InternalApi private[kinesis] (
    val record: KinesisClientRecord,
    val batchData: BatchData,
    val processorData: ShardProcessorData
) {

  val sequenceNumber: String = record.sequenceNumber()
  val subSequenceNumber: Long = record.subSequenceNumber()

  /**
   * Returns the ShutdownReason of the related
   * [[software.amazon.kinesis.processor.ShardRecordProcessor]],
   * if any.
   */
  def shutdownReason: Option[ShutdownReason]

  /**
   * Helper method that tells the caller if it's safe to invoke
   * `forceCheckpoint` or not. It doesn't guarantee that invocations
   * to either `tryToCheckpoint` or `forceCheckpoint` will succeed.
   */
  def canBeCheckpointed: Boolean =
    !shutdownReason.contains(ShutdownReason.LEASE_LOST)

  /**
   * Safe checkpoint method, that will only attempt to checkpoint
   * it the lease has not been lost and will capture expected
   * Exceptions (that may still occur due to unavoidable race
   * conditions). The method will still throw critical Exceptions.
   *
   * This method will potentially perform IO actions. Wrap accordingly
   * in an IO Data Type if needed.
   *
   * See [[software.amazon.kinesis.processor.RecordProcessorCheckpointer]]
   */
  def tryToCheckpoint(): Unit =
    if (canBeCheckpointed) {
      try forceCheckpoint()
      catch {
        case _: ShutdownException => ()
      }
    }

  /**
   * Basic checkpoint method, the caller should decide if it's safe
   * to invoke it. The method will throw any internal Exception.
   *
   * This method will potentially perform IO actions. Wrap accordingly
   * in an IO Data Type if needed.
   *
   * See [[software.amazon.kinesis.processor.RecordProcessorCheckpointer]]
   */
  def forceCheckpoint(): Unit

}

object CommittableRecord {

  // Only makes sense to compare Records belonging to the same shard
  // Records that have been batched by the KPL producer all have the
  // same sequence number but will differ by subsequence number
  implicit val orderBySequenceNumber: Ordering[CommittableRecord] =
    Ordering[(String, Long)].on(cr ⇒ (cr.sequenceNumber, cr.subSequenceNumber))

  /**
   * See [[akka.stream.alpakka.kinesis.impl.ShardProcessor]]
   */
  final class ShardProcessorData(
      val shardId: String,
      val recordProcessorStartingSequenceNumber: ExtendedSequenceNumber,
      val pendingCheckpointSequenceNumber: ExtendedSequenceNumber
  )

  /**
   * See [[akka.stream.alpakka.kinesis.impl.ShardProcessor]]
   */
  final class BatchData(
      val cacheEntryTime: Instant,
      val cacheExitTime: Instant,
      val isAtShardEnd: Boolean,
      val millisBehindLatest: Long
  )
}
