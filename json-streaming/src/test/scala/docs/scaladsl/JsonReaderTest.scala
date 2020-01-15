/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.alpakka.json.scaladsl.JsonReader
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import org.jsfr.json.compiler.JsonPathCompiler
import org.jsfr.json.exception.JsonSurfingException
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.Await
import scala.concurrent.duration._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class JsonReaderTest extends AnyWordSpec with Matchers with BeforeAndAfterAll {
  implicit val system: ActorSystem = ActorSystem("Test")
  implicit val mat: Materializer = ActorMaterializer()

  // The timeout of all streams under test
  val timeout: FiniteDuration = 3.seconds

  // Runs the stream to a sequence sink and returns that sequence
  def collect[A](source: Source[A, _]): Seq[A] = Await.result(source.runWith(Sink.seq), timeout)

  // Basic documents + elements to use in tests
  val expectedElements: Seq[String] = Seq("""{"name":"test1"}""", """{"name":"test2"}""", """{"name":"test3"}""")
  val baseDocument: String =
    s"""
      |{
      |  "size": 3,
      |  "rows": [
      |    {"id": 1, "doc": ${expectedElements(0)}},
      |    {"id": 2, "doc": ${expectedElements(1)}},
      |    {"id": 3, "doc": ${expectedElements(2)}}
      |  ]
      |}
    """.stripMargin

  "JSON parsing support" must {
    "properly parse and push only the elements wanted" in {
      // #usage
      val results = Source
        .single(ByteString.fromString(baseDocument))
        .via(JsonReader.select("$.rows[*].doc"))
        .runWith(Sink.seq)
      // #usage

      val streamed = Await.result(results, timeout)
      streamed shouldBe expectedElements.map(ByteString.fromString)
    }

    "properly parse and push elements of json arriving in very small chunks" in {
      val chunks = baseDocument.grouped(2).toList

      val streamed = collect(Source(chunks.map(ByteString.fromString)).via(JsonReader.select("$.rows[*].doc")))
      streamed shouldBe expectedElements.map(ByteString.fromString)
    }

    "properly parse and push elements of json arriving in larger chunks" in {
      val chunks = baseDocument.grouped(10).toList

      val streamed = collect(Source(chunks.map(ByteString.fromString)).via(JsonReader.select("$.rows[*].doc")))
      streamed shouldBe expectedElements.map(ByteString.fromString)
    }

    "properly parse and stream a json array as the top-level element" in {
      val content = "[1, 2, 3]"

      val streamed = collect(Source.single(ByteString.fromString(content)).via(JsonReader.select("$[*]")))
      streamed shouldBe Seq("1", "2", "3").map(ByteString.fromString)
    }

    "accept a pre-compiled json path rather than a string as a parameter" in {
      val path = JsonPathCompiler.compile("$.rows[*].doc")

      val streamed = collect(Source.single(ByteString.fromString(baseDocument)).via(JsonReader.select(path)))
      streamed shouldBe expectedElements.map(ByteString.fromString)
    }

    "fail the stream if it encounters invalid json" in {
      val doc = "{invalid: json}"

      a[JsonSurfingException] shouldBe thrownBy {
        collect(Source.single(ByteString(doc)).via(JsonReader.select("$.invalid[*]")))
      }
    }

    "fail the stream if it encounters invalid json in the middle of valid values" in {
      // Leaving the "," between the array elements out deliberately
      val brokenChunks = Seq("[", "\"test\"", ",", "\"it\"", "\"breaks\"")

      a[JsonSurfingException] shouldBe thrownBy {
        collect(Source(brokenChunks.toVector.map(ByteString.fromString)).via(JsonReader.select("$[*]")))
      }
    }

    "fail the stream if it parsed several elements successfully but cannot finish parsing properly" in {
      // Deliberately left the closing bracket and brace out to produce an incomplete result
      val chunks = Vector("{", "\"numbers\"", ":", "[", "1", ",", "2")

      a[JsonSurfingException] shouldBe thrownBy {
        collect(Source(chunks.map(ByteString.fromString)).via(JsonReader.select("$.names[*]")))
      }
    }

    "fail early if the given JsonPath is not parseable" in {
      a[RuntimeException] shouldBe thrownBy(JsonReader.select("invalid"))
    }
  }

  override protected def afterAll(): Unit = system.terminate()
}
