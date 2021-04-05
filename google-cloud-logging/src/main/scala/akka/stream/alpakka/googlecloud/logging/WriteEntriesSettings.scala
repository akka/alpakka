/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.logging

import akka.stream.OverflowStrategy
import akka.stream.alpakka.googlecloud.logging.model.{LogEntry, LogSeverity, WriteEntriesRequest}
import akka.util.JavaDurationConverters._

import java.time.Duration
import scala.collection.immutable.Seq
import scala.concurrent.duration.FiniteDuration

/**
 * Configuration for the `writeEntries` sink.
 *
 * @param queueSize        The maximum capacity of the queue used to buffer log entries
 * @param flushSeverity    Buffered log entries get immediately flushed for logs at or above this [[LogSeverity]]
 * @param flushWithin      The maximum time a log entry is queued before it is attempted to be flushed
 * @param overflowStrategy The [[OverflowStrategy]] to use; only [[OverflowStrategy.dropHead]] and [[OverflowStrategy.dropTail]] supported
 * @param requestTemplate  A template [[WriteEntriesRequest]] that will be populated with the log entries
 */
final case class WriteEntriesSettings private (
    queueSize: Int,
    flushSeverity: LogSeverity,
    flushWithin: FiniteDuration,
    overflowStrategy: OverflowStrategy,
    requestTemplate: WriteEntriesRequest[Nothing]
) {

  def getQueueSize = queueSize
  def getFlushSeverity = flushSeverity
  def getflushWithin = flushWithin.asJava
  def getOverflowStrategy = overflowStrategy
  def getRequestTemplate[T] = requestTemplate.withEntries(Seq.empty[LogEntry[T]])

  def withQueueSize(queueSize: Int) =
    copy(queueSize = queueSize)
  def withFlushSeverity(flushSeverity: LogSeverity) =
    copy(flushSeverity = flushSeverity)
  def withflushWithin(flushWithin: FiniteDuration) =
    copy(flushWithin = flushWithin)
  def withflushWithin(flushWithin: Duration) =
    copy(flushWithin = flushWithin.asScala)
  def withOverflowStrategy(overflowStrategy: OverflowStrategy) =
    copy(overflowStrategy = overflowStrategy)
  def withRequestTemplate[T](requestTemplate: WriteEntriesRequest[T]) =
    copy(requestTemplate = requestTemplate.withEntries(Seq.empty))
}

object WriteEntriesSettings {

  /**
   * Java API: Configuration for the `writeEntries` sink.
   *
   * @param queueSize        The maximum capacity of the queue used to buffer log entries
   * @param flushSeverity    Buffered log entries get immediately flushed for logs at or above this [[LogSeverity]]
   * @param flushWithin      The maximum time a log entry is queued before it is attempted to be flushed
   * @param overflowStrategy The [[OverflowStrategy]] to use; only `dropHead` and `dropTail` supported
   * @param requestTemplate  A template [[WriteEntriesRequest]] that will be populated with the log entries
   * @tparam T the data model for the `jsonPayload` of log entries
   * @return the [[WriteEntriesSettings]]
   */
  def create[T](
      queueSize: Int,
      flushSeverity: LogSeverity,
      flushWithin: Duration,
      overflowStrategy: OverflowStrategy,
      requestTemplate: WriteEntriesRequest[T]
  ) =
    WriteEntriesSettings(queueSize,
                         flushSeverity,
                         flushWithin.asScala,
                         overflowStrategy,
                         requestTemplate.withEntries(Seq.empty))

}
