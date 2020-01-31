/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.stream.alpakka.geode.RegionSettings
import akka.stream.alpakka.geode.scaladsl.{Geode, PoolSubscription}
import akka.stream.scaladsl.Sink

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt

class GeodeSinkSpec extends GeodeBaseSpec {

  "Geode sink" should {
    it { geodeSettings =>
      "stores persons in geode" in {

        val geode = new Geode(geodeSettings) with PoolSubscription

        val source = buildPersonsSource(30 to 40)

        val sink = geode.sink(personsRegionSettings)
        val fut = source.runWith(sink)

        Await.ready(fut, 5.seconds)

        geode.close()
      }

      "store animals in geode" in {

        val geode = new Geode(geodeSettings) with PoolSubscription

        val source = buildAnimalsSource(1 to 40)

        //#sink
        val animalsRegionSettings: RegionSettings[Int, Animal] =
          RegionSettings("animals", (a: Animal) => a.id)

        val sink: Sink[Animal, Future[Done]] =
          geode.sink(animalsRegionSettings)

        val fut: Future[Done] = source.runWith(sink)
        //#sink
        Await.ready(fut, 10.seconds)

        geode.close()
      }

      "store complex in geode" in {
        val geode = new Geode(geodeSettings) with PoolSubscription
        val source = buildComplexesSource(1 to 40)
        val sink = geode.sink(complexesRegionSettings)
        val fut = source.runWith(sink)
        Await.ready(fut, 10.seconds)
        geode.close()
      }

    }

  }

}
