/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.util

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class AnnotateLastSpec
    extends TestKit(ActorSystem("AnnotateLastSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  "AnnotateLast" should {

    "indicate last element" in {
      val probe = Source(1 to 3).via(AnnotateLast[Int]).runWith(TestSink.probe)
      probe.requestNext(NotLast(1))
      probe.requestNext(NotLast(2))
      probe.requestNext(Last(3))
      probe.expectComplete()
    }

    "indicate first element is last if only one element" in {
      val probe = Source.single(1).via(AnnotateLast[Int]).runWith(TestSink.probe)
      probe.requestNext(Last(1))
      probe.expectComplete()
    }

    "do nothing when stream is empty" in {
      val probe = Source.empty[Nothing].via(AnnotateLast[Nothing]).runWith(TestSink.probe)
      probe.expectSubscriptionAndComplete()
    }
  }
}
