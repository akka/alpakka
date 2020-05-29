/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import java.time.Instant

import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.must.Matchers
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType

class ShardSettingsSpec extends AnyWordSpec with Matchers with LogCapturing {
  val baseSettings = ShardSettings("name", "shardid")
  "ShardSettings" must {
    "require a timestamp for shard iterator type is AT_TIMESTAMP" in {
      a[IllegalArgumentException] should be thrownBy baseSettings
        .withShardIteratorType(ShardIteratorType.AT_TIMESTAMP)
    }
    "accept a valid timestamp for shard iterator type AT_TIMESTAMP" in {
      noException should be thrownBy baseSettings
        .withAtTimestamp(Instant.now())
        .withShardIteratorType(ShardIteratorType.AT_TIMESTAMP)
    }
    "require a sequence number for iterator type AT_SEQUENCE_NUMBER" in {
      a[IllegalArgumentException] should be thrownBy baseSettings
        .withShardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
    }
    "accept a valid sequence number for iterator type AT_SEQUENCE_NUMBER" in {
      noException should be thrownBy baseSettings
        .withStartingSequenceNumber("SQC")
        .withShardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
    }
    "require a sequence number for iterator type AFTER_SEQUENCE_NUMBER" in {
      a[IllegalArgumentException] should be thrownBy baseSettings
        .withStartingSequenceNumber(null)
        .withShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
    }
    "accept a valid sequence number for iterator type AFTER_SEQUENCE_NUMBER" in {
      noException should be thrownBy baseSettings
        .withStartingSequenceNumber("SQC")
        .withShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
    }
    "require a valid limit" in {
      a[IllegalArgumentException] should be thrownBy baseSettings.withLimit(10001)
      a[IllegalArgumentException] should be thrownBy baseSettings.withLimit(-1)
    }
    "accept a valid limit" in {
      noException should be thrownBy baseSettings.withLimit(500)
    }

    "accept all combinations of alterations with ShardIterator" in {
      noException should be thrownBy baseSettings
        .withShardIterator(ShardIterator.AtSequenceNumber("SQC"))
        .withShardIterator(ShardIterator.AfterSequenceNumber("SQC"))
        .withShardIterator(ShardIterator.AtTimestamp(Instant.EPOCH))
        .withShardIterator(ShardIterator.Latest)
        .withShardIterator(ShardIterator.TrimHorizon)
    }
  }
}
