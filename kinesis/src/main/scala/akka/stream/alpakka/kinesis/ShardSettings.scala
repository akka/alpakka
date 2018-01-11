/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import java.util.Date

import com.amazonaws.services.kinesis.model.ShardIteratorType

import scala.concurrent.duration.FiniteDuration

object ShardSettings {

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
}
