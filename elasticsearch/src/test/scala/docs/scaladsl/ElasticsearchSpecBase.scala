/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.http.scaladsl.{Http, HttpExt}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{BeforeAndAfterAll, Inspectors}

trait ElasticsearchSpecBase
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with IntegrationPatience
    with Inspectors
    with LogCapturing
    with BeforeAndAfterAll {

  //#init-mat
  implicit val system: ActorSystem = ActorSystem()
  //#init-mat
  implicit val http: HttpExt = Http()
}
