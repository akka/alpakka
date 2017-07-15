/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.geode.scaladsl

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class GeodeSinkSpec extends GeodeBaseSpec {

  "Reactive geode sink" should {
    it { geodeSettings =>
      "stores persons in geode" in {

        val reactiveGeode = new ReactiveGeode(geodeSettings) with PoolSubscription

        val source = buildPersonsSource(30 to 40)

        val sink = reactiveGeode.sink(personsRegionSettings)
        val fut = source.runWith(sink)

        Await.ready(fut, 5 seconds)

        reactiveGeode.close()
      }

      "stores animals in geode" in {

        val reactiveGeode = new ReactiveGeode(geodeSettings) with PoolSubscription

        val source = buildAnimalsSource(1 to 40)

        //#sink
        val sink = reactiveGeode.sink(animalsRegionSettings)

        val fut = source.runWith(sink)
        //#sink
        Await.ready(fut, 10 seconds)

        reactiveGeode.close()
      }

      "stores complex in geode" in {

        val reactiveGeode = new ReactiveGeode(geodeSettings) with PoolSubscription

        val source = buildComplexesSource(1 to 40)

        //#sink
        val sink = reactiveGeode.sink(complexesRegionSettings)

        val fut = source.runWith(sink)
        //#sink
        Await.ready(fut, 10 seconds)

        reactiveGeode.close()
      }

    }

  }

}
