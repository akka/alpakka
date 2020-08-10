/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.util

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import scala.concurrent.duration._

class DelaySpec
    extends TestKit(ActorSystem("PageTokenGeneratorSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  implicit val materializer: ActorMaterializer = ActorMaterializer()

  "Delay" should {
    "work as passthrough if should delay returns false" in {

      val elems = 0 to 12
      val probe = Source(elems)
        .map(_ => System.nanoTime())
        .via(Delay(_ => false, 1000, TimeUnit.MILLISECONDS))
        .map(start => System.nanoTime() - start)
        .runWith(TestSink.probe)

      val expectedDelay = 300.milli.dilated

      elems.foreach(_ => {
        val next = probe
          .request(1)
          .expectNext(expectedDelay)

        next should be <= expectedDelay.toNanos
      })

    }

    "delay elements using fibonacci series" in {

      val elems = 0 to 12
      val delays = List(1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144).map(_.millis)
      val probe = Source(elems)
        .map(_ => System.nanoTime())
        .via(Delay(_ => true, 1000, TimeUnit.MILLISECONDS))
        .map(start => System.nanoTime() - start)
        .runWith(TestSink.probe)

      (elems zip delays).foreach {
        case (_, delay) =>
          val next = probe
            .request(1)
            .expectNext()

          next should be >= delay.toNanos
      }

    }

    "throw IllegalStateException when maxDelay is exceeded" in {

      val elems = 0 to 12

      val probe = Source(elems)
        .map(_ => System.nanoTime())
        .via(Delay(_ => true, 10, TimeUnit.MILLISECONDS))
        .map(start => System.nanoTime() - start)
        .runWith(TestSink.probe)

      probe.request(6).expectNextN(6)

      val error = probe.request(1).expectError()
      error shouldBe an[IllegalStateException]
    }

  }

}
