package akka.stream.alpakka.eip

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class SplitterExamples extends FlatSpec with BeforeAndAfterAll with Matchers {

  implicit val system = ActorSystem("Test")
  implicit val mat = ActorMaterializer()
  
  "Simple Splitter" should " split " in {
    val source: Source[String, NotUsed] = Source(List("1-2-3", "2-3", "3-4"))

    //Some Sink
//    val printSink = Sink.foreach[Any](f => {
//      println(f)
//    })

    //Simple Split
    val ret = source.map(s => s.split("-").toList)
      .mapConcat(identity)
      .map(s => s.toInt)
      .runWith(Sink.seq)
    
    ret should be (null)

  }
  

  override protected def afterAll(): Unit = system.terminate()

}
