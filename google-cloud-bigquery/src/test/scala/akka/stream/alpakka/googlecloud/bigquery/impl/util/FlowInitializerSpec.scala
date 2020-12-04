/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.util

import akka.actor.ActorSystem
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class FlowInitializerSpec
    extends TestKit(ActorSystem("PageTokenGeneratorSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override def afterAll: Unit =
    TestKit.shutdownActorSystem(system)

  "FlowInitializer" should {

    "Put initial value in front of the stream" in {
      val sourceProbe = TestSource.probe[String]
      val sinkProbe = TestSink.probe[String]

      val probe = sourceProbe
        .via(FlowInitializer("a"))
        .runWith(sinkProbe)

      probe.requestNext() should be("a")
    }
  }

}
