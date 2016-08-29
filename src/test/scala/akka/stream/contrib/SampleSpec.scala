package akka.stream.contrib

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{Sink, Source}
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.Await
import scala.concurrent.duration._

class SampleSpec extends WordSpec with Matchers {
  private implicit val system = ActorSystem("SampleTest")
  private implicit val materializer = ActorMaterializer()

  "Sample Stage" should {
    "returns every Nth element in stream" in {
      val list = 1 to 1000
      val source = Source.fromIterator[Int](() => list.toIterator)

      for (n <- 1 to 100) {
        val future = source.via(Sample(n)).runWith(Sink.seq)
        val expected = list.filter(_ % n == 0).toList

        Await.result(future, 3 seconds) should ===(expected)
      }
    }

    "returns elements randomly" in {
      // a fake random, increase by 1 for every invocation result
      var num = 0
      val mockRandom = () => {
        num += 1
        num
      }

      val future = Source.fromIterator[Int](() => (1 to 10).toIterator)
        .via(Sample(mockRandom))
        .runWith(Sink.seq)

      Await.result(future, 3 seconds) should ===((1 :: 3 :: 6 :: 10 :: Nil))
    }

    "throws exception when next step <= 0" in {
      intercept[IllegalArgumentException] {
        Await.result(Source.empty.via(Sample(() => 0)).runWith(Sink.seq), 3 seconds)
      }

      intercept[IllegalArgumentException] {
        Await.result(Source.empty.via(Sample(() => -1)).runWith(Sink.seq), 3 seconds)
      }
    }

    "throws exceptions when max random step <= 0" in {
      intercept[IllegalArgumentException] {
        Await.result(Source.empty.via(Sample.random(0)).runWith(Sink.seq), 3 seconds)
      }
    }
  }
}



