/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.nio.file.Paths

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, IOResult, Materializer}
import akka.testkit.TestKit
import akka.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, RecoverMethods}

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.Success
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class CharsetCodingFlowsDoc
    extends TestKit(ActorSystem("charset"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with RecoverMethods
    with LogCapturing {

  private implicit val mat: Materializer = ActorMaterializer()

  val multiByteChars = "äåû經濟商行政管理总局التجارى"

  "Encoding" should {

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

  }

}
