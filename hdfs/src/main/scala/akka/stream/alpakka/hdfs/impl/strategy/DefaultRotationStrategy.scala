/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs.impl.strategy

import akka.annotation.InternalApi
import akka.stream.alpakka.hdfs.RotationStrategy
import akka.stream.alpakka.hdfs.impl.HdfsFlowLogic

import scala.concurrent.duration.FiniteDuration

/**
 * Internal API
 */
@InternalApi
private[hdfs] object DefaultRotationStrategy {
  final case class SizeRotationStrategy(
      bytesWritten: Long,
      maxBytes: Double
  ) extends RotationStrategy {
    def should(): Boolean = bytesWritten >= maxBytes
    def reset(): RotationStrategy = copy(bytesWritten = 0)
    def update(offset: Long): RotationStrategy = copy(bytesWritten = offset)
    protected[hdfs] def preStart[W, I, C](logic: HdfsFlowLogic[W, I, C]): Unit = ()
  }

  final case class CountRotationStrategy(
      messageWritten: Long,
      c: Long
  ) extends RotationStrategy {
    def should(): Boolean = messageWritten >= c
    def reset(): RotationStrategy = copy(messageWritten = 0)
    def update(offset: Long): RotationStrategy = copy(messageWritten = messageWritten + 1)
    protected[hdfs] def preStart[W, I, C](logic: HdfsFlowLogic[W, I, C]): Unit = ()
  }

  final case class TimeRotationStrategy(interval: FiniteDuration) extends RotationStrategy {
    def should(): Boolean = false
    def reset(): RotationStrategy = this
    def update(offset: Long): RotationStrategy = this
    protected[hdfs] def preStart[W, I, C](logic: HdfsFlowLogic[W, I, C]): Unit =
      logic.sharedScheduleFn(interval, interval)
  }

  case object NoRotationStrategy extends RotationStrategy {
    def should(): Boolean = false
    def reset(): RotationStrategy = this
    def update(offset: Long): RotationStrategy = this
    protected[hdfs] def preStart[W, I, C](logic: HdfsFlowLogic[W, I, C]): Unit = ()
  }
}
