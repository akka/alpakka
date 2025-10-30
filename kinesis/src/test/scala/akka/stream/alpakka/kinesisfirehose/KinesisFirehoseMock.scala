/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.kinesisfirehose

import akka.actor.ActorSystem
import org.mockito.Mockito.{mock, reset}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}
import software.amazon.awssdk.services.firehose.FirehoseAsyncClient

import scala.concurrent.Await
import scala.concurrent.duration._

trait KinesisFirehoseMock extends BeforeAndAfterAll with BeforeAndAfterEach { this: Suite =>

  implicit protected val system: ActorSystem = ActorSystem()
  implicit protected val amazonKinesisFirehoseAsync: FirehoseAsyncClient = mock(classOf[FirehoseAsyncClient])

  override protected def beforeEach(): Unit =
    reset(amazonKinesisFirehoseAsync)

  override protected def afterAll(): Unit =
    Await.ready(system.terminate(), 5.seconds)

}
