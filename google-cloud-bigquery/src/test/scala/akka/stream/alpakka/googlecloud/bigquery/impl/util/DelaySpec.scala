/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.util

import akka.actor.ActorSystem
import akka.stream.alpakka.googlecloud.bigquery.RetrySettings
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._

class DelaySpec extends TestKit(ActorSystem("DelaySpec")) with AnyWordSpecLike with Matchers with BeforeAndAfterAll {

  val retrySettings = RetrySettings(0, 1.millisecond, 5000.milliseconds, 0.0)

  "Delay" should {
    "work as passthrough if should delay returns false" in {

      val elems = 0 to 12
      val probe = Source(elems)
        .map(_ => System.nanoTime())
        .via(Delay(retrySettings)(_ => false))
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

    "delay elements using exponential backoff" in {

      val elems = 0 to 12
      val delays = elems.map(1 << _).map(_.millis)
      val probe = Source(elems)
        .map(_ => System.nanoTime())
        .via(Delay(retrySettings)(_ => true))
        .map(start => System.nanoTime() - start)
        .runWith(TestSink.probe)

      (elems zip delays).foreach {
        case (_, delay) =>
          val next = probe
            .request(1)
            .expectNext()

          next should be >= (delay.toNanos - 10.millis.toNanos)
      }

    }

  }

}
