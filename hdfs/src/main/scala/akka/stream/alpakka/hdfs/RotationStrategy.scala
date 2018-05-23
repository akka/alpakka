/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs

import akka.stream.alpakka.hdfs.RotationStrategy._

import scala.concurrent.duration.FiniteDuration

sealed trait RotationStrategy extends Strategy {
  type S = RotationStrategy
}

private[hdfs] object RotationStrategy {
  final case class SizeRotationStrategy(
      bytesWritten: Long,
      maxBytes: Double
  ) extends RotationStrategy {
    def should(): Boolean = bytesWritten >= maxBytes
    def reset(): RotationStrategy = copy(bytesWritten = 0)
    def update(offset: Long): RotationStrategy = copy(bytesWritten = offset)
  }

  final case class CountRotationStrategy(
      messageWritten: Long,
      size: Long
  ) extends RotationStrategy {
    def should(): Boolean = messageWritten >= size
    def reset(): RotationStrategy = copy(messageWritten = 0)
    def update(offset: Long): RotationStrategy = copy(messageWritten = messageWritten + 1)
  }

  final case class TimeRotationStrategy(interval: FiniteDuration) extends RotationStrategy {
    def should(): Boolean = false
    def reset(): RotationStrategy = this
    def update(offset: Long): RotationStrategy = this
  }

  case object NoRotationStrategy extends RotationStrategy {
    def should(): Boolean = false
    def reset(): RotationStrategy = this
    def update(offset: Long): RotationStrategy = this
  }
}

object RotationStrategyFactory {
  /*
   * Creates [[SizeRotationStrategy]]
   */
  def size(count: Double, unit: FileUnit): RotationStrategy = SizeRotationStrategy(0, count * unit.byteCount)

  /*
   * Creates [[CountedRotationStrategy]]
   */
  def count(size: Long): RotationStrategy = CountRotationStrategy(0, size)

  /*
   * Creates [[TimedRotationStrategy]]
   */
  def time(interval: FiniteDuration): RotationStrategy = TimeRotationStrategy(interval)

  /*
   * Creates [[NoRotationStrategy]]
   */
  def none: RotationStrategy = NoRotationStrategy
}
