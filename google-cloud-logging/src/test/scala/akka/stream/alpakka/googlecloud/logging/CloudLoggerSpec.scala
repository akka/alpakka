/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.logging

import akka.actor.ActorSystem
import akka.stream.alpakka.googlecloud.logging.CloudLoggerSpec.config
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import io.specto.hoverfly.junit.core.{HoverflyMode, SimulationSource}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.io.File

object CloudLoggerSpec {

  val `application.conf` =
    """akka.loggers = ["akka.event.Logging$DefaultLogger", "akka.stream.alpakka.googlecloud.logging.CloudLogger"]
      |alpakka.google.logger.resource-type = global
      |""".stripMargin

  val config = ConfigFactory.parseString(`application.conf`).withFallback(ConfigFactory.load())

}

class CloudLoggerSpec
    extends TestKit(ActorSystem("CloudLoggerSpec", config))
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with HoverflySupport {

  override def beforeAll(): Unit = {
    super.beforeAll()
    system.settings.config.getString("alpakka.google.logging.test.e2e-mode") match {
      case "simulate" =>
        hoverfly.simulate(SimulationSource.url(getClass.getClassLoader.getResource("CloudLoggerSpec.json")))
      case "capture" => hoverfly.resetMode(HoverflyMode.CAPTURE)
      case _ => throw new IllegalArgumentException
    }
  }

  override def afterAll() = {
    system.terminate()
    if (hoverfly.getMode == HoverflyMode.CAPTURE)
      hoverfly.exportSimulation(new File("src/test/resources/CloudLoggerSpec.json").toPath)
    super.afterAll()
  }

  "CloudLogger" should {

    "log events" ignore {

      system.log.debug("debug")
      system.log.info("info")
      system.log.warning("warning")
      system.log.error(new Exception("cause"), "error")

      system.actorSelection("")

      Thread.sleep(1000 * 10)

      // TODO This test will always pass; requires human verification
    }

  }

}
