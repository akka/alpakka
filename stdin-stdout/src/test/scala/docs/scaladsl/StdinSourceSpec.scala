/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.util.NoSuchElementException

import akka.actor.ActorSystem
import akka.stream.alpakka.stdinout.StdinSourceReader
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.Sink
import akka.stream.alpakka.stdinout.scaladsl.StdinSource
import akka.stream.alpakka.stdinout.testkit.ReaderFactory
import akka.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Inside, Matchers, WordSpec}

class StdinSourceSpec extends WordSpec with BeforeAndAfterAll with ScalaFutures with Matchers with Inside {

  implicit val sys: ActorSystem = ActorSystem("StdinSourceSpec")
  implicit val mat: Materializer = ActorMaterializer()

  "StdinSource" should {

    "correctly read a single string from its reader" in {
      val singleReader: StdinSourceReader = ReaderFactory.createStdinSourceReaderFromList(Array("abc"))

      val msg: String = StdinSource(singleReader).runWith(Sink.head).futureValue
      msg should equal("abc")
    }

    "correctly read two string messages from its reader" in {
      val doubleReader: StdinSourceReader = ReaderFactory.createStdinSourceReaderFromList(Array("abc", "def"))

      val msgs: Seq[String] = StdinSource(doubleReader).runWith(Sink.seq).futureValue
      msgs should equal(Seq("abc", "def"))
    }

    "correctly read three string messages from its reader" in {
      val tripleReader: StdinSourceReader = ReaderFactory.createStdinSourceReaderFromList(Array("abc", "def", "ghi"))

      val msgs: Seq[String] = StdinSource(tripleReader).runWith(Sink.seq).futureValue
      msgs should equal(Seq("abc", "def", "ghi"))
    }

    "support mapping of individual messages as strings in a downstream akka stream map stage" in {
      val stringReader: StdinSourceReader = ReaderFactory.createStdinSourceReaderFromList(Array("abc", "def", "ghi"))

      val msgs: Seq[String] = StdinSource(stringReader).map(_.toUpperCase).runWith(Sink.seq).futureValue
      msgs should equal(Seq("ABC", "DEF", "GHI"))
    }

    "support mapping valid strings to other types in a downstream akka stream map stage" in {
      val intReader: StdinSourceReader = ReaderFactory.createStdinSourceReaderFromList(Array("1", "2", "3"))

      val msgs: Seq[Int] = StdinSource(intReader).map(_.toInt).runWith(Sink.seq).futureValue
      msgs should equal(Seq(1, 2, 3))
    }

    "Fail the stage if the internal reader throws an error" in {
      val exceptionReader: StdinSourceReader = ReaderFactory.createStdinSourceReaderThrowsException()

      StdinSource(exceptionReader).runWith(Sink.seq).failed.futureValue shouldBe a[NoSuchElementException]
      StdinSource(exceptionReader).runWith(Sink.seq).failed.futureValue.getMessage should equal(
        "example reader failure"
      )
    }

  }

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(sys)

}
