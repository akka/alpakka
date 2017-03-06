package akka.stream.alpakka.csv.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import akka.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpec}

import scala.concurrent.duration.DurationInt

abstract class CsvSpec
  extends WordSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with ScalaFutures {


  implicit val system = ActorSystem(this.getClass.getSimpleName)
  implicit val materializer = ActorMaterializer()

  override protected def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)
}

class CsvFramingSpec extends CsvSpec {
  override implicit val patienceConfig = PatienceConfig(2.seconds)

  "CSV Framing" should {
    "parse one line" in {
      // #line-scanner
      val fut = Source.single(ByteString("eins,zwei,drei"))
        .via(CsvFraming.lineScanner())
        .runWith(Sink.seq)
      // #line-scanner
      fut.futureValue.head should be (List(ByteString("eins"), ByteString("zwei"), ByteString("drei")))
    }

    "parse two lines" in {
      val fut = Source.single(ByteString("eins,zwei,drei\nuno,dos,tres"))
        .via(CsvFraming.lineScanner())
        .runWith(Sink.seq)
      val res = fut.futureValue
      res.head should be (List(ByteString("eins"), ByteString("zwei"), ByteString("drei")))
      res(1) should be (List(ByteString("uno"), ByteString("dos"), ByteString("tres")))
    }

  }
}