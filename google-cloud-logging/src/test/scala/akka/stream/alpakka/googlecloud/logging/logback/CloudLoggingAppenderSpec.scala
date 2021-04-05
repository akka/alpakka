/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.logging.logback

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.googlecloud.logging.HoverflySupport
import akka.stream.alpakka.googlecloud.logging.model.LogEntry
import akka.testkit.TestKit
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.LoggerContext
import io.specto.hoverfly.junit.core.{HoverflyMode, SimulationSource}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

import java.io.File
import scala.concurrent.duration.DurationInt

class CloudLoggingAppenderSpec
    extends TestKit(ActorSystem("CloudLoggingAppenderSpec"))
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with HoverflySupport {

  implicit val patience = PatienceConfig(10.seconds)

  override def beforeAll(): Unit = {
    super.beforeAll()
    system.settings.config.getString("alpakka.google.logging.test.e2e-mode") match {
      case "simulate" =>
        hoverfly.simulate(SimulationSource.url(getClass.getClassLoader.getResource("CloudLoggingAppenderSpec.json")))
      case "capture" => hoverfly.resetMode(HoverflyMode.CAPTURE)
      case _ => throw new IllegalArgumentException
    }
  }

  override def afterAll() = {
    system.terminate()
    if (hoverfly.getMode == HoverflyMode.CAPTURE)
      hoverfly.exportSimulation(new File("src/test/resources/CloudLoggingAppenderSpec.json").toPath)
    super.afterAll()
  }

  "CloudLoggingAppender" should {

    "log events" ignore {

      //#logback-setup
      val context = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]

      val appender = new CloudLoggingAppender
      appender.setContext(context)
      appender.setActorSystem(system)
      appender.setName("cloud")
      appender.setResourceType("global") //#snipper-please-ignore
      appender.addEnhancer(classOf[MDCEventEnhancer].getName)
      appender.addEnhancer(classOf[TestEnhancer].getName)
      appender.start()

      val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
      root.addAppender(appender)
      //#logback-setup

      val logger = LoggerFactory.getLogger(this.getClass)
      logger.trace("trace")
      logger.debug("debug")
      logger.info("info")
      logger.warn("warn")
      logger.error("error", new Exception("cause"))

      val flushed = appender.flushed

      appender.stop()

      flushed.futureValue shouldBe Done

    }

  }

}

class TestEnhancer extends LoggingEnhancer {
  override def enhanceLogEntry[T](entry: LogEntry[T], event: ILoggingEvent): LogEntry[T] = {
    entry.withLabel("specEnhancer", "enhanced")
  }
}
