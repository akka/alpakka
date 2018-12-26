/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import java.util.Date
import java.util.concurrent.TimeUnit

import com.amazonaws.services.kinesis.model.ShardIteratorType

import scala.concurrent.duration.FiniteDuration

object ShardSettings {

  /**
   * Java API: Create settings using the default configuration
   */
  def create(streamName: String, shardId: String): ShardSettings =
    ShardSettings(streamName,
                  shardId,
                  ShardIteratorType.LATEST,
                  None,
                  None,
                  FiniteDuration.apply(1L, TimeUnit.SECONDS),
                  500);

  /**
   * Java API: Create settings using the default configuration
   */
  def create(streamName: String,
             shardId: String,
             shardIteratorType: ShardIteratorType,
             refreshInterval: FiniteDuration,
             limit: Integer) =
    ShardSettings(streamName, shardId, shardIteratorType, None, None, refreshInterval, limit)

  /**
   * Java API: Create settings using a starting sequence number
   */
  def create(streamName: String,
             shardId: String,
             shardIteratorType: ShardIteratorType,
             startingSequenceNumber: String,
             refreshInterval: FiniteDuration,
             limit: Integer) =
    ShardSettings(streamName, shardId, shardIteratorType, Some(startingSequenceNumber), None, refreshInterval, limit)

  /**
   * Java API: Create settings using a timestamp
   */
  def create(streamName: String,
             shardId: String,
             shardIteratorType: ShardIteratorType,
             timestamp: Date,
             refreshInterval: FiniteDuration,
             limit: Integer) =
    ShardSettings(streamName, shardId, shardIteratorType, None, Some(timestamp), refreshInterval, limit)

}

case class ShardSettings(streamName: String,
                         shardId: String,
                         shardIteratorType: ShardIteratorType,
                         startingSequenceNumber: Option[String] = None,
                         atTimestamp: Option[java.util.Date] = None,
                         refreshInterval: FiniteDuration,
                         limit: Int) {
  require(
    limit >= 1 && limit <= 10000,
    "Limit must be between 0 and 10000. See: http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html"
  )
  shardIteratorType match {
    case ShardIteratorType.AFTER_SEQUENCE_NUMBER | ShardIteratorType.AT_SEQUENCE_NUMBER =>
      require(startingSequenceNumber.nonEmpty)
    case ShardIteratorType.AT_TIMESTAMP => require(atTimestamp.nonEmpty)
    case _ => ()
  }

  def withShardIteratorType(shardIteratorType: ShardIteratorType): ShardSettings =
    copy(shardIteratorType = shardIteratorType)

  def withStartingSequenceNumber(startingSequenceNumber: String): ShardSettings =
    copy(startingSequenceNumber = Option(startingSequenceNumber))

  def withAtTimestamp(atTimestamp: java.util.Date): ShardSettings = copy(atTimestamp = Option(atTimestamp))

  def withAtTimestamp(atTimestamp: java.time.ZonedDateTime): ShardSettings =
    copy(atTimestamp = Option(java.util.Date.from(atTimestamp.toInstant)))

  def withRefreshInterval(refreshInterval: Long, unit: TimeUnit): ShardSettings =
    copy(refreshInterval = FiniteDuration(refreshInterval, unit))

  def withLimit(limit: Int): ShardSettings = copy(limit = limit)

}
