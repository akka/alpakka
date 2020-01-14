/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesisfirehose

import scala.util.control.NoStackTrace

object KinesisFirehoseErrors {
  sealed trait KinesisSourceError extends NoStackTrace
  case object NoShardsError extends KinesisSourceError
  case object GetShardIteratorError extends KinesisSourceError
  case object GetRecordsError extends KinesisSourceError

  sealed trait KinesisFlowErrors extends NoStackTrace
  case class FailurePublishingRecords(e: Throwable) extends RuntimeException(e) with KinesisFlowErrors

}
