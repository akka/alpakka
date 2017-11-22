/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.worker

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.clientlibrary.types.{
  ExtendedSequenceNumber,
  InitializationInput,
  ProcessRecordsInput,
  ShutdownInput
}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

private[kinesis] class IRecordProcessor(
    callback: CommittableRecord => Unit
)(implicit executionContext: ExecutionContext)
    extends com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor {
  private var shardId: String = _
  private var extendedSequenceNumber: ExtendedSequenceNumber = _

  var shutdown: Option[ShutdownReason] = None

  override def initialize(initializationInput: InitializationInput): Unit = {
    this.shardId = initializationInput.getShardId
    this.extendedSequenceNumber = initializationInput.getExtendedSequenceNumber
  }

  override def processRecords(processRecordsInput: ProcessRecordsInput): Unit =
    processRecordsInput.getRecords.asScala.foreach { record =>
      callback(
        new CommittableRecord(
          shardId,
          extendedSequenceNumber,
          processRecordsInput.getMillisBehindLatest,
          record,
          recordProcessor = this,
          processRecordsInput.getCheckpointer
        )
      )
    }

  override def shutdown(shutdownInput: ShutdownInput): Unit =
    shutdown = Some(shutdownInput.getShutdownReason)

}
