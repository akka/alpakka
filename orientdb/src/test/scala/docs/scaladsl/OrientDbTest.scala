/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.testkit.{TestKit, TestKitBase}
import com.dimafeng.testcontainers.ForAllTestContainer
import org.scalatest.{BeforeAndAfterAll, Suite}

trait OrientDbTest extends ForAllTestContainer with BeforeAndAfterAll with TestKitBase { self: Suite =>
  override lazy val container = new OrientDbContainer()

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }
}
