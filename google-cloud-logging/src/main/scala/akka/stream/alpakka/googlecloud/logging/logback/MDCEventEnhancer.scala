/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.logging.logback

import akka.stream.alpakka.googlecloud.logging.model.LogEntry
import ch.qos.logback.classic.spi.ILoggingEvent

import scala.collection.JavaConverters._

/**
 * MDCEventEnhancer takes values found in the MDC property map and adds them as labels to the
 * [[LogEntry]]. This [[LoggingEnhancer]] is turned on by default. If you wish to filter which
 * MDC values get added as labels to your [[LogEntry]], implement a [[LoggingEnhancer]]
 * and add its classpath to your `logback.xml`. If any [[LoggingEnhancer]] is added
 * this class is no longer registered.
 */
final class MDCEventEnhancer extends LoggingEnhancer {
  def enhanceLogEntry[T](entry: LogEntry[T], event: ILoggingEvent): LogEntry[T] = {
    entry.withLabels(entry.labels ++ event.getMDCPropertyMap.asScala.toMap)
  }
}
