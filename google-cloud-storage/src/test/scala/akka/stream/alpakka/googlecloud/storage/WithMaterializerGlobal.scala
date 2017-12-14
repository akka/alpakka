/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import org.specs2.specification.AfterAll

import scala.concurrent.Await
import scala.concurrent.duration._

trait WithMaterializerGlobal extends AfterAll {
  //println(" actorSystem start")
  implicit val actorSystem = ActorSystem("test")
  implicit val materializer = ActorMaterializer.create(actorSystem)
  implicit val ec = materializer.executionContext
  private val http = Http(actorSystem)

  override def afterAll = {
    //println(" actorSystem stop")
    http.shutdownAllConnectionPools()
    materializer.shutdown()
    Await.result(actorSystem.terminate(), 10.seconds)
  }
}
