/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.client

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.stream.ActorMaterializer
import io.vertx.core.{Vertx, VertxOptions}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpec}
import scala.concurrent.duration._

trait StompClientSpec extends WordSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with ScalaFutures {

  implicit val system = ActorSystem(this.getClass.getSimpleName)
  implicit val materializer = ActorMaterializer()(system)

  val patience = 2.seconds

  override implicit val patienceConfig = PatienceConfig(patience)
  implicit val executionContext = ExecutionContexts.sameThreadExecutionContext

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }
}
