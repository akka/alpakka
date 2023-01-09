/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis.sink

import akka.stream.alpakka.kinesis.KinesisLimits._

import scala.concurrent.duration.FiniteDuration

case class KinesisSinkSettings(
    streamName: String,
    maxAggRecordBytes: Int,
    maxConcurrentRequests: Int,
    nrOfInstance: Int,
    maxBurstDuration: FiniteDuration,
    maxBatchRequestBytes: Int,
    maxBufferedBytes: Option[Int],
    maxBufferedDuration: Option[FiniteDuration]
) {
  require(
    maxBatchRequestBytes <= PutRecordsMaxBytes,
    s"PutRecords requests must not exceed ${PutRecordsMaxBytes >> 20} MiB"
  )

  require(
    maxAggRecordBytes <= MaxBytesPerRecord,
    s"Aggregated record must not exceed ${MaxBytesPerRecord >> 20} MiB"
  )

  require(
    maxBufferedBytes.nonEmpty || maxBufferedDuration.nonEmpty,
    "Either maxBufferedBytes or maxBufferedDuration must be specified"
  )
}
