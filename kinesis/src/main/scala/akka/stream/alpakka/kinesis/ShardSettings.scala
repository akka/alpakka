/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import com.amazonaws.services.kinesis.model.ShardIteratorType

import scala.concurrent.duration._
import akka.util.JavaDurationConverters._

final class ShardSettings private (
    val streamName: String,
    val shardId: String,
    val shardIteratorType: com.amazonaws.services.kinesis.model.ShardIteratorType,
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
  def withShardIteratorType(value: ShardIteratorType): ShardSettings = copy(shardIteratorType = value)

  /**
   * Sets `shardIteratorType` to `AT_SEQUENCE_NUMBER` and uses the given value as starting sequence number.
   */
  def withStartingSequenceNumber(value: String): ShardSettings =
    copy(shardIteratorType = ShardIteratorType.AT_SEQUENCE_NUMBER, startingSequenceNumber = Option(value))

  /**
   * Sets `shardIteratorType` to `AFTER_SEQUENCE_NUMBER` and uses the given value as starting sequence number.
   */
  def withStartingAfterSequenceNumber(value: String): ShardSettings =
    copy(shardIteratorType = ShardIteratorType.AFTER_SEQUENCE_NUMBER, startingSequenceNumber = Option(value))

  /**
   * Sets `shardIteratorType` to `AT_TIMESTAMP` and uses the given `Instant` as starting timestamp.
   *
   * @deprecated prefer java.time.Instant to provide the timeout, since 1.0-M3
   */
  @deprecated("prefer java.time.Instant to provide the timeout", "1.0-M3")
  def withAtTimestamp(value: java.util.Date): ShardSettings =
    copy(shardIteratorType = ShardIteratorType.AT_TIMESTAMP, atTimestamp = Option(value.toInstant))

  /**
   * Sets `shardIteratorType` to `AT_TIMESTAMP` and uses the given `Instant` as starting timestamp.
   */
  def withAtTimestamp(value: java.time.Instant): ShardSettings =
    copy(shardIteratorType = ShardIteratorType.AT_TIMESTAMP, atTimestamp = Option(value))

  /** Scala API */
  def withRefreshInterval(value: scala.concurrent.duration.FiniteDuration): ShardSettings =
    copy(refreshInterval = value)

  /** Java API */
  def withRefreshInterval(value: java.time.Duration): ShardSettings = copy(refreshInterval = value.asScala)
  def withLimit(value: Int): ShardSettings = copy(limit = value)

  private def copy(
      streamName: String = streamName,
      shardId: String = shardId,
      shardIteratorType: com.amazonaws.services.kinesis.model.ShardIteratorType = shardIteratorType,
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
    new ShardSettings(streamName,
                      shardId,
                      ShardIteratorType.LATEST,
                      startingSequenceNumber = None,
                      atTimestamp = None,
                      refreshInterval = 1.second,
                      limit = 500)

  /**
   * Java API: Create settings using the default configuration
   */
  def create(streamName: String, shardId: String): ShardSettings = apply(streamName, shardId)

}
