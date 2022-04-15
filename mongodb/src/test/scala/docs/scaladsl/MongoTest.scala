/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.testkit.TestKitBase
import com.dimafeng.testcontainers.{ForAllTestContainer, MongoDBContainer}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait MongoTest extends ForAllTestContainer with TestKitBase with BeforeAndAfterAll { self: Suite =>
  override lazy val container = new MongoDBContainer()

  lazy val ConnectionString: String =
    s"mongodb://${container.container.getContainerIpAddress}:${container.container.getMappedPort(27017)}"

  override protected def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }
}
