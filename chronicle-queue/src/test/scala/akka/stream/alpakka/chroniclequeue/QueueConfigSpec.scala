/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

// ORIGINAL LICENCE
/*
 *  Copyright 2017 PayPal
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package akka.stream.alpakka.chroniclequeue

import com.typesafe.config.ConfigFactory
import net.openhft.chronicle.queue.RollCycles
import net.openhft.chronicle.wire.WireType
import org.scalatest.{FlatSpec, Matchers}

import akka.stream.alpakka.chroniclequeue.scaladsl._

class QueueConfigSpec extends FlatSpec with Matchers {

  it should "properly read the configuration from config" in {

    val configText =
      """
        | persist-dir = /tmp/myQueue
        | roll-cycle = xlarge_daily
        | wire-type = compressed_binary
        | block-size = 80m
        | index-count = 131072
        | index-spacing = 8k
        | output-ports = 3
        | strict-commit-order = true
      """.stripMargin
    val config = ConfigFactory.parseString(configText)
    val queueConfig = ChronicleQueueConfig.from(config)
    queueConfig.persistDir.getAbsolutePath shouldBe "/tmp/myQueue"
    queueConfig.rollCycle shouldBe RollCycles.XLARGE_DAILY
    queueConfig.wireType shouldBe WireType.COMPRESSED_BINARY
    queueConfig.blockSize shouldBe (80 * 1024 * 1024)
    queueConfig.indexSpacing shouldBe (8 * 1024)
    queueConfig.indexCount shouldBe RollCycles.XLARGE_DAILY.defaultIndexCount
    queueConfig.isBuffered shouldBe false
    queueConfig.epoch shouldBe 0L
    queueConfig.outputPorts shouldBe 3
    queueConfig.strictCommitOrder shouldBe true
  }

  it should "properly assume default configurations" in {

    val configText =
      """
        | persist-dir = /tmp/myQueue
      """.stripMargin
    val config =
      ConfigFactory.parseString(configText).withFallback(ConfigFactory.load().getConfig("alpakka.chroniclequeue"))
    val queueConfig = ChronicleQueueConfig.from(config)
    queueConfig.persistDir.getAbsolutePath shouldBe "/tmp/myQueue"
    queueConfig.rollCycle shouldBe RollCycles.DAILY
    queueConfig.wireType shouldBe WireType.BINARY
    queueConfig.blockSize shouldBe (64 * 1024 * 1024)
    queueConfig.indexSpacing shouldBe RollCycles.DAILY.defaultIndexSpacing
    queueConfig.indexCount shouldBe RollCycles.DAILY.defaultIndexCount
    queueConfig.isBuffered shouldBe false
    queueConfig.epoch shouldBe 0L
    queueConfig.outputPorts shouldBe 1
    queueConfig.strictCommitOrder shouldBe false
  }

  it should "set commit order policy to lenient" in {
    val configText =
      """
        | persist-dir = /tmp/myQueue
        | roll-cycle = xlarge_daily
        | wire-type = compressed_binary
        | block-size = 80m
        | index-count = 131072
        | index-spacing = 8k
        | output-ports = 3
        | strict-commit-order = false
      """.stripMargin
    val config = ConfigFactory.parseString(configText)
    val queueConfig = ChronicleQueueConfig.from(config)
    queueConfig.strictCommitOrder shouldBe false
  }
}
