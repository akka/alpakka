/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package docs.scaladsl

import akka.actor.{ActorSystem, ClassicActorSystemProvider}
import akka.stream.Materializer
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

abstract class CsvSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with ScalaFutures
    with LogCapturing {

  implicit val system: ActorSystem = ActorSystem(this.getClass.getSimpleName)
  implicit val classic: ClassicActorSystemProvider = system
  implicit val mat: Materializer = Materializer(system)

  override protected def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)
}
