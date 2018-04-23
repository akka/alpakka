/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.text.scaladsl

import java.nio.charset.{Charset, StandardCharsets, UnmappableCharacterException}
import java.nio.file.Paths

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.{ActorMaterializer, IOResult, Materializer}
import akka.testkit.TestKit
import akka.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, RecoverMethods, WordSpecLike}

import scala.collection.immutable
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.concurrent.duration.DurationInt
import scala.util.Success

class CharsetCodingFlowsSpec
    extends TestKit(ActorSystem("charset"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with RecoverMethods {

  private implicit val mat: Materializer = ActorMaterializer()
  private implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val multiByteChars = "äåû經濟商行政管理总局التجارى"

  "Encoding" should {
    def verifyEncoding(charsetOut: Charset, value: String) = {
      val result = Source
        .single(value)
        .via(TextFlow.encoding(charsetOut))
        .map(_.decodeString(charsetOut))
        .runWith(Sink.head)
      result.futureValue should be(value)
    }
    "work for UTF-16" in {
      verifyEncoding(StandardCharsets.UTF_16, multiByteChars)
    }

    "fail for non-representable chars" in {
      recoverToSucceededIf[UnmappableCharacterException] {
        Source
          .single("經濟部")
          .via(TextFlow.encoding(StandardCharsets.US_ASCII))
          .runWith(Sink.ignore)
      }
    }

    "be illustrated in a documentation example" in {
      // format: off
      // #encoding
      import java.nio.charset.StandardCharsets
      import akka.stream.alpakka.text.scaladsl.TextFlow
      import akka.stream.scaladsl.FileIO

      // #encoding
      import scala.collection.JavaConverters._
      val targetFile = Paths.get("target/outdata.txt")
      val strings = System.getProperties.asScala.map(p => p._1 + " -> " + p._2).toList
      // #encoding
      val stringSource: Source[String, _] = // ...
        // #encoding
        Source(strings)
      val result =
        // #encoding

      stringSource
        .via(TextFlow.encoding(StandardCharsets.US_ASCII))
        .intersperse(ByteString("\n"))
        .runWith(FileIO.toPath(targetFile))
      // #encoding
      result.futureValue.status should be(Success(Done))
      // format: on
    }
  }

  "Decoding" should {
    "be illustrated in a documentation example" in {
      // format: off
      // #decoding
      import java.nio.charset.StandardCharsets
      import akka.stream.alpakka.text.scaladsl.TextFlow

      // #decoding
      val utf16bytes = ByteString("äåûßêëé", StandardCharsets.UTF_16)
      // #decoding
      val byteStringSource: Source[ByteString, _] = // ...
        // #decoding
        Source
          .single(utf16bytes)
      // #decoding

      val result: Future[immutable.Seq[String]] =
        byteStringSource
          .via(TextFlow.decoding(StandardCharsets.UTF_16))
          .runWith(Sink.seq)
      // #decoding
      result.futureValue should be(Seq("äåûßêëé"))
      // format: on
    }

  }

  "Transcoding" should {
    "be illustrated in a documentation example" in {
      // format: off
      // #transcoding
      import java.nio.charset.StandardCharsets
      import akka.stream.scaladsl.FileIO
      import akka.stream.alpakka.text.scaladsl.TextFlow

      // #transcoding
      val utf16bytes = ByteString("äåûßêëé", StandardCharsets.UTF_16)
      val targetFile = Paths.get("target/outdata-transcoding.txt")
      // #transcoding
      val byteStringSource: Source[ByteString, _] = // ...
      // #transcoding
        Source
          .single(utf16bytes)
      val result: Future[IOResult] =
      // #transcoding

      byteStringSource
        .via(TextFlow.transcoding(StandardCharsets.UTF_16, StandardCharsets.UTF_8))
        .runWith(FileIO.toPath(targetFile))
      // #transcoding
      result.futureValue.status should be(Success(Done))
      // format: on
    }

    def verifyTranscoding(charsetIn: Charset, charsetOut: Charset, value: String) = {
      val result = Source
        .single(value)
        .map(s => ByteString(s, charsetIn))
        .via(TextFlow.transcoding(charsetIn, charsetOut))
        .map(_.decodeString(charsetOut))
        .runWith(Sink.head)
      result.futureValue should be(value)
    }

    def verifyByteSends(charsetIn: Charset, charsetOut: Charset, in: String) = {
      val (source, sink) = TestSource
        .probe[ByteString]
        .via(TextFlow.transcoding(charsetIn, charsetOut))
        .map(_.decodeString(charsetOut))
        .toMat(Sink.seq)(Keep.both)
        .run()

      val bs = ByteString(in, charsetIn)
      bs.sliding(1).foreach { bs =>
        source.sendNext(bs)
      }
      source.sendComplete()
      sink.futureValue.mkString should be(in)
    }

    "work for UTF-16" in {
      verifyTranscoding(StandardCharsets.UTF_16, StandardCharsets.UTF_16, multiByteChars)
    }

    "work for UTF-16BE to LE" in {
      verifyTranscoding(StandardCharsets.UTF_16BE, StandardCharsets.UTF_16LE, multiByteChars)
    }

    "work for Windows-1252 to UTF-16LE" in {
      verifyTranscoding(Charset.forName("windows-1252"), StandardCharsets.UTF_16LE, "äåûßêëé")
    }

    "complete" in {
      val (source, sink) = TestSource
        .probe[ByteString]
        .via(TextFlow.transcoding(StandardCharsets.UTF_8, StandardCharsets.UTF_8))
        .toMat(TestSink.probe[ByteString])(Keep.both)
        .run()
      source.sendNext(ByteString("eins,zwei,drei"))
      sink.request(3)
      sink.expectNext(ByteString("eins,zwei,drei"))
      sink.expectNoMessage(100.millis)
      source.sendComplete()
      sink.expectComplete()
    }

    "work byte by byte from UTF-16LE" in {
      verifyByteSends(StandardCharsets.UTF_16LE, StandardCharsets.UTF_8, multiByteChars)
    }

    "work for byte by byte windows-1252" in {
      verifyByteSends(Charset.forName("windows-1252"), StandardCharsets.UTF_8, "äåûßêëé")
    }
  }

}
