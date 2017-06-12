/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.kairosdb.scaladsl

import java.io.IOException

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSource
import org.kairosdb.client.HttpClient
import org.kairosdb.client.builder.MetricBuilder
import org.kairosdb.client.response.Response
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.scalatest.mockito.MockitoSugar.mock

import scala.concurrent.duration._
import scala.concurrent.Await

/**
 * Created by SOROOSH on 3/20/17.
 */
class KairosSinkSpec extends WordSpec with Matchers with BeforeAndAfter {

  //#init-mat
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  //#init-mat

  implicit val client = mock[HttpClient]

  before {
    reset(client)
  }
  "KairosSink" should {
    "send a metric via http client" in {
      when(client.pushMetrics(any())).thenReturn(new Response(200))

      val (probe, future) = TestSource.probe[MetricBuilder].toMat(KairosSink())(Keep.both).run()
      val builder = MetricBuilder.getInstance()
      probe.sendNext(builder).sendComplete()
      Await.result(future, 1 second) shouldBe Done

      verify(client, times(1)).pushMetrics(any())
    }

    "fail stage on http client error" in {
      when(client.pushMetrics(any())).thenThrow(new IOException("Fake IO error"))

      val (probe, future) = TestSource.probe[MetricBuilder].toMat(KairosSink())(Keep.both).run()
      val builder = MetricBuilder.getInstance()

      probe.sendNext(builder).sendComplete()
      an[IOException] should be thrownBy {
        Await.result(future, 1 second)
      }

      verify(client, times(1)).pushMetrics(any())
    }

    "call client for each metric" in {
      when(client.pushMetrics(any())).thenReturn(new Response(200))

      val (probe, future) = TestSource.probe[MetricBuilder].toMat(KairosSink())(Keep.both).run()
      val builder = MetricBuilder.getInstance()
      probe.sendNext(builder).sendNext(builder).sendNext(builder).sendComplete()
      Await.result(future, 1 second) shouldBe Done

      verify(client, times(3)).pushMetrics(any())
    }
  }

}
