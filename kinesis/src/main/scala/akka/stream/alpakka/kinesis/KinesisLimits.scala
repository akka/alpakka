/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import java.math.BigInteger

object KinesisLimits {
  val MaxBytesPerRecord = 1024 * 1024
  val MinExplicitHashCode = BigInteger.ZERO
  val MaxExplicitHashCode = new BigInteger("FF" * 16, 16)
  val MaxRecordsPerSecondPerShard: Int = 1000
  val MaxBytesPerSecondPerShard: Int = 1024 * 1024
  val PutRecordsMaxRecords: Int = 500
  val PutRecordsMaxBytes: Int = 5 * 1024 * 1024
}
