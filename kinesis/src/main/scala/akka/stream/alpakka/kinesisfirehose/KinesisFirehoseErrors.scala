/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesisfirehose

import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResponseEntry

import scala.util.control.NoStackTrace

object KinesisFirehoseErrors {
  sealed trait KinesisSourceError extends NoStackTrace
  case object NoShardsError extends KinesisSourceError
  case object GetShardIteratorError extends KinesisSourceError
  case object GetRecordsError extends KinesisSourceError

  sealed trait KinesisFlowErrors extends NoStackTrace
  case class FailurePublishingRecords(e: Exception) extends RuntimeException(e) with KinesisFlowErrors
  case class ErrorPublishingRecords(attempts: Int, records: Seq[PutRecordBatchResponseEntry])
      extends RuntimeException(s"Unable to publish records after $attempts attempts")
      with KinesisFlowErrors

}
