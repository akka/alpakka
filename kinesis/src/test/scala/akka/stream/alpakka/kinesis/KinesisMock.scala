/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.kinesis

import akka.actor.ActorSystem
import org.mockito.Mockito.{mock, reset}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient

import scala.concurrent.Await
import scala.concurrent.duration._

trait KinesisMock extends BeforeAndAfterAll with BeforeAndAfterEach { this: Suite =>

  implicit protected val system: ActorSystem = ActorSystem()
  implicit protected val amazonKinesisAsync: KinesisAsyncClient = mock(classOf[KinesisAsyncClient])

  override protected def beforeEach(): Unit =
    reset(amazonKinesisAsync)

  override protected def afterAll(): Unit =
    Await.ready(system.terminate(), 5.seconds)

}
