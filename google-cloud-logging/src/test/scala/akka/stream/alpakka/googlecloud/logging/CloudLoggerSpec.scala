/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.logging

import akka.actor.{ActorSystem, Props}
import akka.event.Logging._
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class CloudLoggerSpec
    extends TestKit(ActorSystem("CloudLoggerSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  override def afterAll() = {
    system.terminate()
  }

  "CloudLogger" should {

    "log events" ignore {

      val probe = TestProbe()
      val logger = probe.childActorOf(Props(classOf[CloudLogger]))
      probe.send(logger, InitializeLogger(system.eventStream))
      probe.expectMsg(LoggerInitialized)
      AllLogLevels.foreach(l => system.eventStream.subscribe(logger, classFor(l)))

      system.log.debug("debug")
      system.log.info("info")
      system.log.warning("warning")
      system.log.error(new Exception("cause"), "error")

      system.eventStream.subscribe(probe.ref, classOf[Info])
      system.stop(logger)

      probe.fishForMessage() {
        case Info(_, _, "CloudLogger stopped") => true
        case _ => false
      }
    }

  }

}
