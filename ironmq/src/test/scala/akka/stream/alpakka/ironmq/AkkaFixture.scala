/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ironmq

import akka.actor.ActorSystem
import org.scalatest.{ BeforeAndAfterEach, Suite }

trait AkkaFixture extends ConfigFixture with BeforeAndAfterEach { _: Suite =>

  private var mutableActorSystem = Option.empty[ActorSystem]
  implicit def actorSystem: ActorSystem =
    mutableActorSystem.getOrElse(throw new IllegalArgumentException("The ActorSystem is not initialized"))

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    mutableActorSystem = Option(ActorSystem(s"test-${System.currentTimeMillis()}", config))
  }

  override protected def afterEach(): Unit = {
    actorSystem.terminate()
    super.afterEach()
  }
}
