package akka.stream.alpakka.kinesis

import java.math.BigInteger

object KinesisLimits {
  val MaxBytesPerRecord = 1024 * 1024
  val MinExplicitHashCode = BigInteger.ZERO
  val MaxExplicitHashCode = new BigInteger("FF" * 16, 16)
}
