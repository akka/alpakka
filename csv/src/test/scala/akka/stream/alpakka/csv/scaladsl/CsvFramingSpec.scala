/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.csv.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import akka.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpec}
import scala.collection.immutable.Seq
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
      val fut = Source.single(ByteString("eins,zwei,drei\n")).via(CsvFraming.lineScanner()).runWith(Sink.seq)
      // #line-scanner
      fut.futureValue.head should be(List(ByteString("eins"), ByteString("zwei"), ByteString("drei")))
    }

    "parse two lines" in {
      val fut =
        Source.single(ByteString("eins,zwei,drei\nuno,dos,tres\n")).via(CsvFraming.lineScanner()).runWith(Sink.seq)
      val res = fut.futureValue
      res.head should be(List(ByteString("eins"), ByteString("zwei"), ByteString("drei")))
      res(1) should be(List(ByteString("uno"), ByteString("dos"), ByteString("tres")))
    }

    "parse semicolon lines" in {
      val fut =
        Source.single(ByteString(
          """eins;zwei;drei
            |ein”s;zw ei;dr\ei
            |un’o;dos;tres
          """.stripMargin))
          .via(CsvFraming.lineScanner(delimiter = ';', escapeChar = '*'))
          .map(_.map(_.utf8String))
          .runWith(Sink.seq)
      val res = fut.futureValue
      res.head should be(List("eins", "zwei", "drei"))
      res(1) should be(List("ein”s", "zw ei", "dr\\ei"))
    }

    "parse chunks successfully" in {
      val input = Seq(
        "eins,zw",
        "ei,drei\nuno",
        ",dos,tres\n"
      ).map(ByteString(_))
      val fut = Source.apply(input).via(CsvFraming.lineScanner()).map(_.map(_.utf8String)).runWith(Sink.seq)
      val res = fut.futureValue
      res.head should be(List("eins", "zwei", "drei"))
      res(1) should be(List("uno", "dos", "tres"))

    }
  }
}
