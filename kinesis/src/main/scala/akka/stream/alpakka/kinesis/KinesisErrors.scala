/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import scala.util.control.NoStackTrace

object KinesisErrors {

  sealed trait KinesisSourceError extends NoStackTrace
  case object NoShardsError extends KinesisSourceError
  class GetShardIteratorError(val shardId: String, e: Throwable)
      extends RuntimeException(s"Failed to get a shard iterator for shard [$shardId]. Reason : ${e.getMessage}", e)
      with KinesisSourceError
  class GetRecordsError(val shardId: String, e: Throwable)
      extends RuntimeException(s"Failed to fetch records from Kinesis for shard [$shardId]. Reason : ${e.getMessage}",
                               e
      )
      with KinesisSourceError

  sealed trait KinesisFlowErrors extends NoStackTrace
  case class FailurePublishingRecords(e: Throwable)
      extends RuntimeException(s"Failure publishing records to Kinesis. Reason : ${e.getMessage}", e)
      with KinesisFlowErrors
}
