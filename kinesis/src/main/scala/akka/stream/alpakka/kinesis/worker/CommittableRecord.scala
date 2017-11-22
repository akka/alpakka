/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.worker

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber
import com.amazonaws.services.kinesis.model.Record

import scala.concurrent.{ExecutionContext, Future}

class CommittableRecord(
    val shardId: String,
    val recordProcessorStartingSequenceNumber: ExtendedSequenceNumber,
    val millisBehindLatest: Long,
    val record: Record,
    recordProcessor: IRecordProcessor,
    checkpointer: IRecordProcessorCheckpointer
)(implicit executor: ExecutionContext) {

  val sequenceNumber: String = record.getSequenceNumber

  def recordProcessorShutdownReason(): Option[ShutdownReason] =
    recordProcessor.shutdown
  def canBeCheckpointed(): Boolean =
    recordProcessorShutdownReason().isEmpty
  def checkpoint(): Future[Unit] =
    Future(checkpointer.checkpoint(record))

}

object CommittableRecord {

  implicit val orderBySequenceNumber: Ordering[CommittableRecord] = Ordering.by(_.sequenceNumber)

}
