/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.s3.scaladsl

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
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

  override protected def afterAll(): Unit = {
    Http(system)
      .shutdownAllConnectionPools()
      .foreach(_ => TestKit.shutdownActorSystem(system))(system.dispatcher)
  }

}
