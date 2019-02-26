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
package akka.stream.alpakka.chroniclequeue.scaladsl

import java.io.File

import com.typesafe.config.Config
import net.openhft.chronicle.queue.{RollCycle, RollCycles}
import net.openhft.chronicle.wire.WireType

object ChronicleQueueConfig {
  val defaultCycle: RollCycle = RollCycles.DAILY
  val defaultWireType: WireType = WireType.BINARY
  val defaultBlockSize: Long = 64L << 20
  val defaultOutputPort: Int = 1
  val defaultStrictCommitOrder = false

  def from(config: Config): ChronicleQueueConfig = {
    val persistDir = new File(config.getString("persist-dir"))
    val cycle = RollCycles.valueOf(config.getString("roll-cycle").toUpperCase)
    val wireType = WireType.valueOf(config.getString("wire-type").toUpperCase)
    val blockSize = config.getMemorySize("block-size").toBytes
    val indexSpacing = config.getMemorySize("index-spacing").toBytes.toInt
    val indexCount = config.getInt("index-count")
    val outputPorts = config.getInt("output-ports")
    val commitOrder = config.getBoolean("strict-commit-order")
    ChronicleQueueConfig(
      persistDir,
      cycle,
      wireType,
      blockSize,
      indexSpacing,
      indexCount,
      outputPorts = outputPorts,
      strictCommitOrder = commitOrder
    )
  }
}

case class ChronicleQueueConfig(
    persistDir: File,
    rollCycle: RollCycle = ChronicleQueueConfig.defaultCycle,
    wireType: WireType = ChronicleQueueConfig.defaultWireType,
    blockSize: Long = ChronicleQueueConfig.defaultBlockSize,
    indexSpacing: Int = ChronicleQueueConfig.defaultCycle.defaultIndexSpacing,
    indexCount: Int = ChronicleQueueConfig.defaultCycle.defaultIndexCount,
    isBuffered: Boolean = false,
    epoch: Long = 0L,
    outputPorts: Int = ChronicleQueueConfig.defaultOutputPort,
    strictCommitOrder: Boolean = ChronicleQueueConfig.defaultStrictCommitOrder
)
