/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import java.time.Instant

import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.must.Matchers

class ShardSettingsSpec extends AnyWordSpec with Matchers with LogCapturing {
  val baseSettings = ShardSettings("name", "shardid")
  "ShardSettings" must {
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
