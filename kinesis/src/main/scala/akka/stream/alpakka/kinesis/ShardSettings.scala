/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import software.amazon.awssdk.services.kinesis.model.ShardIteratorType

import scala.concurrent.duration._
import akka.util.JavaDurationConverters._

final class ShardSettings private (
    val streamName: String,
    val shardId: String,
    val shardIteratorType: software.amazon.awssdk.services.kinesis.model.ShardIteratorType,
    val startingSequenceNumber: Option[String],
    val atTimestamp: Option[java.time.Instant],
    val refreshInterval: scala.concurrent.duration.FiniteDuration,
    val limit: Int
) {
  require(
    limit >= 1 && limit <= 10000,
    "Limit must be between 0 and 10000. See: http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html"
  )
  shardIteratorType match {
    case ShardIteratorType.AFTER_SEQUENCE_NUMBER | ShardIteratorType.AT_SEQUENCE_NUMBER =>
      require(
        startingSequenceNumber.nonEmpty,
        "a starting sequence number must be set (try using just `withStartingSequenceNumber` or `withStartingAfterSequenceNumber`)"
      )
    case ShardIteratorType.AT_TIMESTAMP =>
      require(atTimestamp.nonEmpty, "a timestamp must be set (try using just `withAtTimestamp`)")
    case _ => ()
  }

  def withStreamName(value: String): ShardSettings = copy(streamName = value)
  def withShardId(value: String): ShardSettings = copy(shardId = value)

  def withShardIterator(shardIterator: ShardIterator): ShardSettings = copy(
    shardIteratorType = shardIterator.shardIteratorType,
    atTimestamp = shardIterator.timestamp,
    startingSequenceNumber = shardIterator.startingSequenceNumber
  )

  /** Scala API */
  def withRefreshInterval(value: scala.concurrent.duration.FiniteDuration): ShardSettings =
    copy(refreshInterval = value)

  /** Java API */
  def withRefreshInterval(value: java.time.Duration): ShardSettings = copy(refreshInterval = value.asScala)
  def withLimit(value: Int): ShardSettings = copy(limit = value)

  private def copy(
      streamName: String = streamName,
      shardId: String = shardId,
      shardIteratorType: software.amazon.awssdk.services.kinesis.model.ShardIteratorType = shardIteratorType,
      startingSequenceNumber: Option[String] = startingSequenceNumber,
      atTimestamp: Option[java.time.Instant] = atTimestamp,
      refreshInterval: scala.concurrent.duration.FiniteDuration = refreshInterval,
      limit: Int = limit
  ): ShardSettings = new ShardSettings(
    streamName = streamName,
    shardId = shardId,
    shardIteratorType = shardIteratorType,
    startingSequenceNumber = startingSequenceNumber,
    atTimestamp = atTimestamp,
    refreshInterval = refreshInterval,
    limit = limit
  )

  override def toString =
    "ShardSettings(" +
    s"streamName=$streamName," +
    s"shardId=$shardId," +
    s"shardIteratorType=$shardIteratorType," +
    s"startingSequenceNumber=$startingSequenceNumber," +
    s"atTimestamp=$atTimestamp," +
    s"refreshInterval=${refreshInterval.toCoarsest}," +
    s"limit=$limit" +
    ")"
}

object ShardSettings {

  /**
   * Create settings using the default configuration
   */
  def apply(streamName: String, shardId: String): ShardSettings =
    apply(streamName, shardId, ShardIterator.Latest)

  def apply(streamName: String, shardId: String, shardIterator: ShardIterator): ShardSettings =
    new ShardSettings(
      streamName,
      shardId,
      shardIterator.shardIteratorType,
      startingSequenceNumber = shardIterator.startingSequenceNumber,
      atTimestamp = shardIterator.timestamp,
      refreshInterval = 1.second,
      limit = 500
    )

  /**
   * Java API: Create settings using the default configuration
   */
  def create(streamName: String, shardId: String): ShardSettings = apply(streamName, shardId)

}
