/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.geode.scaladsl

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Sink}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class GeodeFlowSpec extends GeodeBaseSpec {

  "Reactive geode" should {
    it { geodeSettings =>
      "flow with shapeless pdx serializer" in {
        //#connection
        val reactiveGeode = new ReactiveGeode(geodeSettings)
        //#connection

        val source = buildPersonsSource(1 to 10)

        //#flow
        val flow: Flow[Person, Person, NotUsed] = reactiveGeode.flow(personsRegionSettings)

        val fut = source.via(flow).runWith(Sink.ignore)
        //#flow
        Await.ready(fut, 10 seconds)

        reactiveGeode.close()
      }
      "flow with explicit pdx serializer" in {
        val reactiveGeode = new ReactiveGeode(geodeSettings)

        val source = buildPersonsSource(1 to 20)

        val flow: Flow[Person, Person, NotUsed] = reactiveGeode.flow(personsRegionSettings, PersonPdxSerializer)

        val fut = source.via(flow).runWith(Sink.ignore)
        Await.ready(fut, 10 seconds)

        reactiveGeode.close()
      }
    }
  }
}
