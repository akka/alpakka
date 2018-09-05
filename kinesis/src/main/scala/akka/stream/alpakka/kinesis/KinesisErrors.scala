/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import com.amazonaws.services.kinesis.model.PutRecordsResultEntry

import scala.util.control.NoStackTrace

object KinesisErrors {

  sealed trait KinesisSourceError extends NoStackTrace
  case object NoShardsError extends KinesisSourceError
  case object GetShardIteratorError extends KinesisSourceError
  case object GetRecordsError extends KinesisSourceError

  sealed trait KinesisFlowErrors extends NoStackTrace
  case class FailurePublishingRecords(e: Exception) extends RuntimeException(e) with KinesisFlowErrors
  case class ErrorPublishingRecords[T](attempts: Int, recordsWithContext: Seq[(PutRecordsResultEntry, T)])
      extends RuntimeException(s"Unable to publish records after $attempts attempts")
      with KinesisFlowErrors {
    val records = recordsWithContext.map(_._1)
  }

}
