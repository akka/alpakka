/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.logging.logback

import akka.stream.alpakka.googlecloud.logging.model.LogEntry
import ch.qos.logback.classic.spi.ILoggingEvent

/**
 * An enhancer for [[ILoggingEvent]] log entries. Used to add custom labels to the [[LogEntry]].
 */
@FunctionalInterface
trait LoggingEnhancer {
  def enhanceLogEntry[T](entry: LogEntry[T], event: ILoggingEvent): LogEntry[T]
}
