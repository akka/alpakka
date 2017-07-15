/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.geode.scaladsl

import akka.stream.scaladsl.Sink
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class GeodeFiniteSourceSpec extends GeodeBaseSpec {

  private val log = LoggerFactory.getLogger(classOf[GeodeFiniteSourceSpec])

  "Geode finite source" should {
    it { geodeSettings =>
      "retrieves finite elements from geode" in {

        val reactiveGeode = new ReactiveGeode(geodeSettings)

        //#query
        val source =
          reactiveGeode
            .query[Person](s"select * from /persons order by id")
            .runWith(Sink.foreach(e => log.debug(s"$e")))
        //#query
        Await.ready(source, 10 seconds)

        val animals =
          reactiveGeode
            .query[Animal](s"select * from /animals order by id")
            .runWith(Sink.foreach(e => log.debug(s"$e")))

        Await.ready(animals, 10 seconds)

        val complexes =
          reactiveGeode
            .query[Complex](s"select * from /complexes order by id")
            .runWith(Sink.foreach(e => log.debug(s"$e")))

        Await.ready(complexes, 10 seconds)

        reactiveGeode.close()
      }
    }
  }
}
