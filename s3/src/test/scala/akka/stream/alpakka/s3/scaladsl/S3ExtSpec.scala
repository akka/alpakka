/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.scaladsl

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpecLike, Matchers}
import akka.stream.alpakka.s3.{Proxy, S3Ext}

import scala.collection.JavaConverters._

/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
class S3ExtSpec extends FlatSpecLike with Matchers {
  it should "reuse application config from actor system" in {
    val config = ConfigFactory.parseMap(
      Map(
        "alpakka.s3.proxy.host" -> "localhost",
        "alpakka.s3.proxy.port" -> 8001,
        "alpakka.s3.proxy.secure" -> false,
        "alpakka.s3.path-style-access" -> true
      ).asJava
    )
    implicit val system = ActorSystem.create("s3", config)
    val ext = S3Ext(system)
    ext.settings.proxy shouldBe Some(Proxy("localhost", 8001, "http"))
    ext.settings.pathStyleAccess shouldBe true
  }
}
