package akka.stream.contrib

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.Source
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.Await
import scala.concurrent.duration._

class SampleSpec extends WordSpec with Matchers
{
  private implicit val system       = ActorSystem("SampleTest")
  private implicit val materializer = ActorMaterializer()

  "Sample Stage" should {
    "returns every Nth element in stream" in {
      val list = 1 to 1000
      val source = Source.fromIterator[Int](() => list.toIterator)

      for (n <- 1 to 100) {
        val future = source.via(Sample(n)).runFold(List.empty[Int])((list, e) => e :: list)
        val expected = list.filter(_ % n == 0).reverse.toList

        Await.result(future, 200 millis) should ===(expected)
      }
    }

    "returns elements randomly" in {
      // a fake random, increase by 1 for every invocation result
      var num = 0
      val mockRandom = () => {
        num += 1
        num
      }

      val expected = (1 :: 3 :: 6 :: 10 :: Nil).reverse
      val source = Source.fromIterator[Int](() => (1 to 10).toIterator)
      val future = source.via(Sample(mockRandom)).runFold(List.empty[Int])((list, e) => e :: list)

      Await.result(future, 200 millis) should ===(expected)
    }
  }
}



