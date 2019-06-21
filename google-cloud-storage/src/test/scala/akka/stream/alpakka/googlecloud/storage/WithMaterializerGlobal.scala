/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}

import scala.concurrent.Await
import scala.concurrent.duration._

trait WithMaterializerGlobal
    extends WordSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with ScalaFutures
    with IntegrationPatience
    with Matchers {
  implicit val actorSystem = ActorSystem("test")
  implicit val materializer = ActorMaterializer()
  implicit val ec = materializer.executionContext

  override def afterAll(): Unit = {
    super.afterAll()
    Await.result(actorSystem.terminate(), 10.seconds)
  }
}
