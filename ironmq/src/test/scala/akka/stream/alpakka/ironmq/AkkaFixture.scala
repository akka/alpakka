/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ironmq

import akka.actor.ActorSystem
import org.scalatest.{BeforeAndAfterEach, Suite}

import scala.concurrent.Await
import scala.concurrent.duration._

trait AkkaFixture extends ConfigFixture with BeforeAndAfterEach { _: Suite =>
  import AkkaFixture._

  /**
   * Override to tune the time the test will wait for the actor system to terminate.
   */
  def actorSystemTerminateTimeout: Duration = DefaultActorSystemTerminateTimeout

  private var mutableActorSystem = Option.empty[ActorSystem]
  implicit def actorSystem: ActorSystem =
    mutableActorSystem.getOrElse(throw new IllegalArgumentException("The ActorSystem is not initialized"))

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    mutableActorSystem = Option(ActorSystem(s"test-${System.currentTimeMillis()}", config))
  }

  override protected def afterEach(): Unit = {
    Await.result(actorSystem.terminate(), actorSystemTerminateTimeout)
    super.afterEach()
  }
}

object AkkaFixture {
  val DefaultActorSystemTerminateTimeout: Duration = 10.seconds
}
