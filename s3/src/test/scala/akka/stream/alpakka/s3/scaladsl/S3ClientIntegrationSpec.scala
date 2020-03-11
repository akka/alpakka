/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.testkit.TestKit
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

trait S3ClientIntegrationSpec
    extends AnyFlatSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Matchers
    with ScalaFutures
    with IntegrationPatience
    with LogCapturing {

  implicit val system: ActorSystem
  implicit val materializer = ActorMaterializer()

  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

}
