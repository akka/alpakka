/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.eventbridge

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import org.mockito.Mockito.reset
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient

import scala.concurrent.Await
import scala.concurrent.duration._

trait DefaultTestContext extends BeforeAndAfterAll with BeforeAndAfterEach with MockitoSugar {
  this: Suite =>

  implicit protected val system: ActorSystem = ActorSystem()
  implicit protected val mat: Materializer = ActorMaterializer()
  implicit protected val eventBridgeClient: EventBridgeAsyncClient = mock[EventBridgeAsyncClient]

  override protected def beforeEach(): Unit =
    reset(eventBridgeClient)

  override protected def afterAll(): Unit =
    Await.ready(system.terminate(), 5.seconds)

}
