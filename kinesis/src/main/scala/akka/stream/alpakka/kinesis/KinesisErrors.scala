/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry

import scala.util.control.NoStackTrace

object KinesisErrors {

  sealed trait KinesisSourceError extends NoStackTrace
  case object NoShardsError extends KinesisSourceError
  class GetShardIteratorError(val shardId: String, e: Throwable)
      extends RuntimeException(s"Failed to get a shard iterator for shard [$shardId]", e)
      with KinesisSourceError
  class GetRecordsError(val shardId: String, e: Throwable)
      extends RuntimeException(s"Failed to fetch records from Kinesis for shard [$shardId]", e)
      with KinesisSourceError

  sealed trait KinesisFlowErrors extends NoStackTrace
  case class FailurePublishingRecords(e: Throwable)
      extends RuntimeException("Failure publishing records to Kinesis", e)
      with KinesisFlowErrors
}
