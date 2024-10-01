/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.testkit

import akka.annotation.InternalApi
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import org.slf4j.LoggerFactory

/**
 * See https://doc.akka.io/docs/akka/current/typed/testing-async.html#silence-logging-output-from-tests
 *
 * INTERNAL API
 */
@InternalApi private[akka] object CapturingAppender {
  import LogbackUtil._

  // make sure logging is booted
  private val dummy = LoggerFactory.getLogger("logcapture")
  private val CapturingAppenderName = "CapturingAppender"

  dummy.debug("enabling CapturingAppender")

  def get(loggerName: String): CapturingAppender = {
    val logbackLogger = getLogbackLogger(loggerName)
    logbackLogger.getAppender(CapturingAppenderName) match {
      case null =>
        throw new IllegalStateException(
          s"$CapturingAppenderName not defined for [${loggerNameOrRoot(loggerName)}] in logback-test.xml"
        )
      case appender: CapturingAppender => appender
      case other =>
        throw new IllegalStateException(s"Unexpected $CapturingAppender: $other")
    }
  }

}

/**
 * See https://doc.akka.io/docs/akka/current/typed/testing-async.html#silence-logging-output-from-tests
 *
 * INTERNAL API
 *
 * Logging from tests can be silenced by this appender. When there is a test failure
 * the captured logging events are flushed to the appenders defined for the
 * akka.actor.testkit.typed.internal.CapturingAppenderDelegate logger.
 *
 * The flushing on test failure is handled by [[akka.actor.testkit.typed.scaladsl.LogCapturing]]
 * for ScalaTest and [[akka.actor.testkit.typed.javadsl.LogCapturing]] for JUnit.
 *
 * Use configuration like the following the logback-test.xml:
 *
 * {{{
 *     <appender name="CapturingAppender" class="akka.actor.testkit.typed.internal.CapturingAppender" />
 *
 *     <logger name="akka.actor.testkit.typed.internal.CapturingAppenderDelegate" >
 *       <appender-ref ref="STDOUT"/>
 *     </logger>
 *
 *     <root level="DEBUG">
 *         <appender-ref ref="CapturingAppender"/>
 *     </root>
 * }}}
 */
@InternalApi private[akka] class CapturingAppender extends AppenderBase[ILoggingEvent] {
  import LogbackUtil._

  private var buffer: Vector[ILoggingEvent] = Vector.empty

  // invocations are synchronized via doAppend in AppenderBase
  override def append(event: ILoggingEvent): Unit = {
    event.prepareForDeferredProcessing()
    buffer :+= event
  }

  /**
   * Flush buffered logging events to the output appenders
   * Also clears the buffer..
   */
  def flush(): Unit = flush(None)

  /**
   * Flush buffered logging events to the output appenders
   *
   *  When sourceActorSystem is set, the log message is only forwarded when it
   *  matches the MDC of the log message
   *
   * Also clears the buffer..
   */
  def flush(sourceActorSystem: Option[String]): Unit = synchronized {
    import scala.jdk.CollectionConverters._
    val logbackLogger = getLogbackLogger(classOf[CapturingAppender].getName + "Delegate")
    val appenders = logbackLogger.iteratorForAppenders().asScala.filterNot(_ == this).toList
    for (event <- buffer; appender <- appenders) {
      if (sourceActorSystem.isEmpty
          || event.getMDCPropertyMap.get("sourceActorSystem") == null
          || sourceActorSystem.contains(event.getMDCPropertyMap.get("sourceActorSystem"))) {
        appender.doAppend(event)
      }
    }
    clear()
  }

  /**
   * Discards the buffered logging events without output.
   */
  def clear(): Unit = synchronized {
    buffer = Vector.empty
  }

}
