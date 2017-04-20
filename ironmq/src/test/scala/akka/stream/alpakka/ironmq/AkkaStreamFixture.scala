/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ironmq

import akka.stream.{ActorMaterializer, Materializer}
import org.scalatest.{BeforeAndAfterEach, Suite}

trait AkkaStreamFixture extends AkkaFixture with BeforeAndAfterEach { _: Suite =>

  private var mutableMaterializer = Option.empty[ActorMaterializer]

  implicit def materializer: Materializer =
    mutableMaterializer.getOrElse(throw new IllegalStateException("Materializer not initialized"))

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    mutableMaterializer = Option(ActorMaterializer())
  }

  override protected def afterEach(): Unit = {
    mutableMaterializer.foreach(_.shutdown())
    super.afterEach()
  }
}
