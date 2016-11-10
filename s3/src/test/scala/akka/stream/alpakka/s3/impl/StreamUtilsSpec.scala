package akka.stream.alpakka.s3.impl

import org.scalatest.FlatSpecLike
import org.scalatest.Matchers
import org.scalatest.concurrent.ScalaFutures

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.ActorMaterializerSettings
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit

class StreamUtilsSpec(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers with ScalaFutures {
  def this() = this(ActorSystem("StreamUtilsSpec"))

  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withDebugLogging(true))

  "counter" should "increment starting from 0" in {
    val testSource = StreamUtils.counter()
    testSource.runWith(TestSink.probe[Int]).request(2).expectNext(0, 1)
  }

  it should "allow specifying an initial value" in {
    val testSource = StreamUtils.counter(5)
    testSource.runWith(TestSink.probe[Int]).request(3).expectNext(5, 6, 7)
  }

}
