/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest._

trait S3ClientIntegrationSpec
    extends FlatSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Matchers
    with ScalaFutures
    with IntegrationPatience {

  implicit val system: ActorSystem
  implicit val materializer = ActorMaterializer()

  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

}
