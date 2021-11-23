/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import java.time.Instant

import software.amazon.awssdk.services.kinesis.model.ShardIteratorType

sealed trait ShardIterator {
  def timestamp: Option[Instant]
  def startingSequenceNumber: Option[String]
  def shardIteratorType: ShardIteratorType
}
object ShardIterator {

  case object Latest extends ShardIterator {
    override final val timestamp: Option[Instant] = None

    override final val startingSequenceNumber: Option[String] = None

    override final val shardIteratorType: ShardIteratorType = ShardIteratorType.LATEST
  }

  case object TrimHorizon extends ShardIterator {
    override final val timestamp: Option[Instant] = None

    override final val startingSequenceNumber: Option[String] = None

    override final val shardIteratorType: ShardIteratorType = ShardIteratorType.TRIM_HORIZON
  }

  case class AtTimestamp private (value: Instant) extends ShardIterator {
    override final val timestamp: Option[Instant] = Some(value)

    override final val startingSequenceNumber: Option[String] = None

    override final val shardIteratorType: ShardIteratorType = ShardIteratorType.AT_TIMESTAMP
  }

  case class AtSequenceNumber(sequenceNumber: String) extends ShardIterator {
    override final val timestamp: Option[Instant] = None

    override final val startingSequenceNumber: Option[String] = Some(sequenceNumber)

    override final val shardIteratorType: ShardIteratorType = ShardIteratorType.AT_SEQUENCE_NUMBER
  }

  case class AfterSequenceNumber(sequenceNumber: String) extends ShardIterator {
    override final val timestamp: Option[Instant] = None

    override final val startingSequenceNumber: Option[String] = Some(sequenceNumber)

    override final val shardIteratorType: ShardIteratorType = ShardIteratorType.AFTER_SEQUENCE_NUMBER
  }
}

/**
 * Java API
 */
object ShardIterators {

  /**
   * Java API
   */
  def latest(): ShardIterator =
    ShardIterator.Latest

  /**
   * Java API
   */
  def trimHorizon(): ShardIterator =
    ShardIterator.TrimHorizon

  /**
   * Java API
   */
  def atTimestamp(timestamp: Instant): ShardIterator =
    ShardIterator.AtTimestamp(timestamp)

  /**
   * Java API
   */
  def atSequenceNumber(value: String): ShardIterator =
    ShardIterator.AtSequenceNumber(value)

  /**
   * Java API
   */
  def afterSequenceNumber(value: String): ShardIterator =
    ShardIterator.AfterSequenceNumber(value)
}
