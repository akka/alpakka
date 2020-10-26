/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.amqp

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.stream.ActorMaterializer
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

abstract class AmqpSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll with ScalaFutures with LogCapturing {

  implicit val system = ActorSystem(this.getClass.getSimpleName)
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = ExecutionContexts.parasitic

  override protected def afterAll(): Unit =
    system.terminate()
}
