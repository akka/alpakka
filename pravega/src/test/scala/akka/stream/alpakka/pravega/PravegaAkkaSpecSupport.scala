/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, Suite}

trait PravegaAkkaSpecSupport extends BeforeAndAfterAll {
  this: Suite =>
  implicit val system = ActorSystem("PravegaSpec")

  override def afterAll() =
    TestKit.shutdownActorSystem(system)

}
