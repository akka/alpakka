/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.caffeine.scaladsl

import akka.actor.ActorSystem
import akka.stream.alpakka.caffeine._
import akka.stream.scaladsl.{GraphDSL, Sink, Source}
import akka.stream.{ActorMaterializer, SourceShape}
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, DurationInt}
import scala.language.postfixOps

//#events
sealed trait Event {
  def id: String
}

case class EventA(id: String, data: String) extends Event

case class EventB(id: String, data: String) extends Event

case class EventC(id: String, data: String) extends Event
//#events

//#aggregated-event
case class AggregatedEvents(key: String, a: Option[EventA] = None, b: Option[EventB] = None, c: Option[EventC] = None) {
  def isComplete: Boolean = a.isDefined && b.isDefined && c.isDefined
}
//#aggregated-event

class CaffeineTestSuite extends WordSpec with Matchers {

  implicit val system = ActorSystem("test")
  implicit val materializer = ActorMaterializer()

  //#aggregator
  val aggregator = new Aggregator[String, Event, AggregatedEvents] {
    override def newAggregate(key: String): AggregatedEvents = AggregatedEvents(key)

    override def inKey(v: Event): String = v.id

    override def outKey(r: AggregatedEvents): String = r.key

    override def combine(r: AggregatedEvents, v: Event): AggregatedEvents = v match {
      case b: EventA => r.copy(a = Some(b))
      case w: EventB => r.copy(b = Some(w))
      case c: EventC => r.copy(c = Some(c))
    }

    override def isComplete(r: AggregatedEvents): Boolean =
      r.isComplete

    override def expiration = 100 millis
  }
  //#aggregator
  def source(to: Int) = Source(1 to to).mapConcat { id =>
    if (id % 3 != 0)
      List(EventA(s"id_$id", "a a"), EventB(s"id_$id", "a b")) //Uncomplete events
    else
      List(EventA(s"id_$id", "a a"), EventB(s"id_$id", "a b"), EventC(s"id_$id", "a c")) // Complete events
  }

  val rev = "\b" * 21

  "CaffeineFanOutSage" should {
    "aggregate events in flow" in {

      var compConnt = 0L
      var expiredCount = 0L

      val f = source(10)
        .via(CaffeineFlow(aggregator))
        .runWith(Sink.foreach[Either[AggregatedEvents, AggregatedEvents]] { r =>
          r match {
            case Right(_) => compConnt = compConnt + 1
            case Left(_) => expiredCount = expiredCount + 1
          }
          print(f"$rev$compConnt%10d $expiredCount%10d")
        })

      Await.result(f, Duration.Inf)

      compConnt shouldEqual 3
      expiredCount shouldEqual 7

    }

    "aggregate event in graph" in {

      var compConnt = 0L
      var expiredCount = 0L

      val g = Source.fromGraph(GraphDSL.create() { implicit builder =>
        import GraphDSL.Implicits._

        val reco = builder.add(CaffeineFlow.tee(aggregator))
        source(10) ~> reco.in

        reco.out1 ~> Sink.foreach[AggregatedEvents] { r =>
          expiredCount = expiredCount + 1
          print(f"$rev$compConnt%10d $expiredCount%10d")
        }
        SourceShape(reco.out0)
      })
      val f = g.runWith(Sink.foreach[AggregatedEvents] { r =>
        compConnt = compConnt + 1
        print(f"$rev$compConnt%10d $expiredCount%10d")
      })

      f.onComplete {
        case e =>
          system.terminate()
      }

      Await.result(f, Duration.Inf)

      compConnt shouldEqual 3
      expiredCount shouldEqual 7

      println("\nDone")

    }

  }

}
