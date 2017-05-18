/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

trait S3ClientIntegrationSpec
    extends FlatSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with IntegrationPatience {

  implicit val system: ActorSystem
  implicit val materializer = ActorMaterializer()
}
