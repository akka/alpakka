/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.testkit.scaladsl

import akka.stream.alpakka.testkit.CapturingAppender

import scala.util.control.NonFatal
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Outcome
import org.scalatest.TestSuite
import org.slf4j.LoggerFactory
import org.slf4j.MDC

/**
 * See https://doc.akka.io/docs/akka/current/typed/testing-async.html#silence-logging-output-from-tests
 *
 * Mixin this trait to a ScalaTest test to make log lines appear only when the test failed.
 *
 * Requires Logback and configuration like the following the logback-test.xml:
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
trait LogCapturing extends BeforeAndAfterAll { self: TestSuite =>

  // Can be overridden to filter on sourceActorSystem
  def sourceActorSytem: Option[String] = None

  // eager access of CapturingAppender to fail fast if misconfigured
  private val capturingAppender = CapturingAppender.get("")

  private val myLogger = LoggerFactory.getLogger(classOf[LogCapturing])

  override protected def afterAll(): Unit = {
    try {
      super.afterAll()
    } catch {
      case NonFatal(e) =>
        myLogger.error("Exception from afterAll", e)
        capturingAppender.flush()
    } finally {
      capturingAppender.clear()
    }
  }

  abstract override def withFixture(test: NoArgTest): Outcome = {
    sourceActorSytem.foreach(MDC.put("sourceActorSystem", _))
    myLogger.info(s"Logging started for test [${self.getClass.getName}: ${test.name}]")
    sourceActorSytem.foreach(_ => MDC.remove("sourceActorSystem"))
    val res = test()
    sourceActorSytem.foreach(MDC.put("sourceActorSystem", _))
    myLogger.info(s"Logging finished for test [${self.getClass.getName}: ${test.name}] that [$res]")
    sourceActorSytem.foreach(_ => MDC.remove("sourceActorSystem"))

    if (!(res.isSucceeded || res.isPending)) {
      println(
        s"--> [${Console.BLUE}${self.getClass.getName}: ${test.name}${Console.RESET}] Start of log messages of test that [$res]"
      )
      capturingAppender.flush(sourceActorSytem)
      println(
        s"<-- [${Console.BLUE}${self.getClass.getName}: ${test.name}${Console.RESET}] End of log messages of test that [$res]"
      )
    }

    res
  }
}
