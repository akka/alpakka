/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import com.amazonaws.services.kinesis.model.PutRecordsResultEntry

import scala.util.control.NoStackTrace

object KinesisErrors {

  sealed trait KinesisSourceError extends NoStackTrace
  case object NoShardsError extends KinesisSourceError
  class GetShardIteratorError(val shardId: String, e: Exception)
      extends RuntimeException(s"Failed to get a shard iterator for shard [$shardId]", e)
      with KinesisSourceError
  class GetRecordsError(val shardId: String, e: Exception)
      extends RuntimeException(s"Failed to fetch records from Kinesis for shard [$shardId]", e)
      with KinesisSourceError

  sealed trait KinesisFlowErrors extends NoStackTrace
  case class FailurePublishingRecords(e: Exception)
      extends RuntimeException("Failure publishing records to Kinesis", e)
      with KinesisFlowErrors
  case class ErrorPublishingRecords[T](attempts: Int, recordsWithContext: Seq[(PutRecordsResultEntry, T)])
      extends RuntimeException(s"Unable to publish records after $attempts attempts")
      with KinesisFlowErrors {
    val records = recordsWithContext.map(_._1)
  }

}
