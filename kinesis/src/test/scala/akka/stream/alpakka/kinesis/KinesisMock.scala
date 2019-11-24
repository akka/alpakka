/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.kinesis

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import org.mockito.Mockito.reset
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient

import scala.concurrent.Await
import scala.concurrent.duration._

trait KinesisMock extends BeforeAndAfterAll with BeforeAndAfterEach with MockitoSugar { this: Suite =>

  implicit protected val system: ActorSystem = ActorSystem()
  implicit protected val materializer: Materializer = ActorMaterializer()
  implicit protected val amazonKinesisAsync: KinesisAsyncClient = mock[KinesisAsyncClient]

  override protected def beforeEach(): Unit =
    reset(amazonKinesisAsync)

  override protected def afterAll(): Unit =
    Await.ready(system.terminate(), 5.seconds)

}
