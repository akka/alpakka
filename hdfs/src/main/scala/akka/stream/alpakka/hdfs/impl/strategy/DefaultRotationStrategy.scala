/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs.impl.strategy

import akka.stream.alpakka.hdfs.RotationStrategy

import scala.concurrent.duration.FiniteDuration

private[hdfs] object DefaultRotationStrategy {
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
      c: Long
  ) extends RotationStrategy {
    def should(): Boolean = messageWritten >= c
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
