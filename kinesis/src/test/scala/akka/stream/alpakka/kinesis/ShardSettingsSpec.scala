/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import java.util.Date

import com.amazonaws.services.kinesis.model.ShardIteratorType
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration._

class ShardSettingsSpec extends WordSpec with Matchers {
  val baseSettings = ShardSettings("name", "shardid", ShardIteratorType.TRIM_HORIZON, None, None, 1.second, 500)
  "ShardSettings" must {
    "require a timestamp for shard iterator type is AT_TIMESTAMP" in {
      a[IllegalArgumentException] should be thrownBy baseSettings.copy(shardIteratorType =
                                                                         ShardIteratorType.AT_TIMESTAMP,
                                                                       atTimestamp = None)
    }
    "accept a valid timestamp for shard iterator type AT_TIMESTAMP" in {
      noException should be thrownBy baseSettings.copy(shardIteratorType = ShardIteratorType.AT_TIMESTAMP,
                                                       atTimestamp = Some(new Date()))
    }
    "require a sequence number for iterator type AT_SEQUENCE_NUMBER" in {
      a[IllegalArgumentException] should be thrownBy baseSettings.copy(shardIteratorType =
                                                                         ShardIteratorType.AT_SEQUENCE_NUMBER,
                                                                       startingSequenceNumber = None)
    }
    "accept a valid sequence number for iterator type AT_SEQUENCE_NUMBER" in {
      noException should be thrownBy baseSettings.copy(shardIteratorType = ShardIteratorType.AT_SEQUENCE_NUMBER,
                                                       startingSequenceNumber = Some("SQC"))
    }
    "require a sequence number for iterator type AFTER_SEQUENCE_NUMBER" in {
      a[IllegalArgumentException] should be thrownBy baseSettings.copy(shardIteratorType =
                                                                         ShardIteratorType.AFTER_SEQUENCE_NUMBER,
                                                                       startingSequenceNumber = None)
    }
    "accept a valid sequence number for iterator type AFTER_SEQUENCE_NUMBER" in {
      noException should be thrownBy baseSettings.copy(shardIteratorType = ShardIteratorType.AFTER_SEQUENCE_NUMBER,
                                                       startingSequenceNumber = Some("SQC"))
    }
    "require a valid limit" in {
      a[IllegalArgumentException] should be thrownBy baseSettings.copy(limit = 10001)
      a[IllegalArgumentException] should be thrownBy baseSettings.copy(limit = -1)
    }
    "accept a valid limit" in {
      noException should be thrownBy baseSettings.copy(limit = 500)
    }
  }
}
