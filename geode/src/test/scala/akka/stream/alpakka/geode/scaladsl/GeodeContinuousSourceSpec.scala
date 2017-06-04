/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.geode.scaladsl

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Sink}
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class GeodeContinuousSourceSpec extends GeodeBaseSpec {

  private val log = LoggerFactory.getLogger(classOf[GeodeContinuousSourceSpec])

  "Geode continuousQuery" should {
    it { geodeSettings =>
      "retrieves continuously elements from geode" in {

        //#connection-with-pool
        val reactiveGeode = new ReactiveGeode(geodeSettings) with PoolSubscription
        //#connection-with-pool

        val flow: Flow[Person, Person, NotUsed] = reactiveGeode.flow(personsRegionSettings)

        //#continuousQuery
        val source =
          reactiveGeode
            .continuousQuery[Person]('test, s"select * from /persons")
            .runWith(Sink.fold(0) { (c, p) =>
              log.debug(s"$p $c")
              if (c == 19) {
                reactiveGeode.closeContinuousQuery('test).foreach { _ =>
                  log.debug("test cQuery is closed")
                }

              }
              c + 1
            })
        //#continuousQuery

        val f = buildPersonsSource(1 to 20)
          .via(flow) //geode flow
          .runWith(Sink.ignore)

        Await.result(f, 10 seconds)

        Await.result(source, 5 seconds)

        reactiveGeode.close()
      }
    }
  }
}
