/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.nio.file.Paths

import akka.NotUsed
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.stream.scaladsl.{FileIO, Flow, Keep, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.util.ByteString

import scala.collection.immutable.Seq
import scala.concurrent.duration.DurationInt

class CsvParsingSpec extends CsvSpec {

  def documentation(): Unit = {
    import CsvParsing._
    val delimiter: Byte = Comma
    val quoteChar: Byte = DoubleQuote
    val escapeChar: Byte = Backslash
    // format: off
    // #flow-type
    import akka.stream.alpakka.csv.scaladsl.CsvParsing

    val flow: Flow[ByteString, List[ByteString], NotUsed]
      = CsvParsing.lineScanner(delimiter, quoteChar, escapeChar)
    // #flow-type
    // format: on
    Source.single(ByteString("a,b,c")).via(flow).runWith(Sink.ignore)
  }

  "CSV parsing" should {
    "parse one line" in assertAllStagesStopped {
      // #line-scanner
      import akka.stream.alpakka.csv.scaladsl.CsvParsing

      // #line-scanner
      val fut =
        // format: off
      // #line-scanner
      Source.single(ByteString("eins,zwei,drei\n"))
        .via(CsvParsing.lineScanner())
        .runWith(Sink.head)
      // #line-scanner
      // format: on
      val result = fut.futureValue
      // #line-scanner

      result should be(List(ByteString("eins"), ByteString("zwei"), ByteString("drei")))
      // #line-scanner
    }

    "parse one line and map to String" in assertAllStagesStopped {
      // #line-scanner-string
      import akka.stream.alpakka.csv.scaladsl.CsvParsing

      // #line-scanner-string
      val fut =
        // format: off
      // #line-scanner-string
      Source.single(ByteString("eins,zwei,drei\n"))
        .via(CsvParsing.lineScanner())
        .map(_.map(_.utf8String))
        .runWith(Sink.head)
      // #line-scanner-string
      // format: on
      val result = fut.futureValue
      // #line-scanner-string

      result should be(List("eins", "zwei", "drei"))
      // #line-scanner-string
    }

    "parse two lines" in assertAllStagesStopped {
      val fut =
        Source.single(ByteString("eins,zwei,drei\nuno,dos,tres\n")).via(CsvParsing.lineScanner()).runWith(Sink.seq)
      val res = fut.futureValue
      res.head should be(List(ByteString("eins"), ByteString("zwei"), ByteString("drei")))
      res(1) should be(List(ByteString("uno"), ByteString("dos"), ByteString("tres")))
    }

    "parse two lines even witouth line end" in assertAllStagesStopped {
      val fut =
        Source.single(ByteString("eins,zwei,drei\nuno,dos,tres")).via(CsvParsing.lineScanner()).runWith(Sink.seq)
      val res = fut.futureValue
      res.head should be(List(ByteString("eins"), ByteString("zwei"), ByteString("drei")))
      res(1) should be(List(ByteString("uno"), ByteString("dos"), ByteString("tres")))
    }

    "parse semicolon lines" in assertAllStagesStopped {
      val fut =
        Source
          .single(ByteString("""eins;zwei;drei
            |ein”s;zw ei;dr\ei
            |un’o;dos;tres
          """.stripMargin))
          .via(CsvParsing.lineScanner(delimiter = ';', escapeChar = '*'))
          .map(_.map(_.utf8String))
          .runWith(Sink.seq)
      val res = fut.futureValue
      res.head should be(List("eins", "zwei", "drei"))
      res(1) should be(List("ein”s", "zw ei", "dr\\ei"))
    }

    "parse chunks successfully" in assertAllStagesStopped {
      val input = Seq(
        "eins,zw",
        "ei,drei\nuno",
        ",dos,tres\n"
      ).map(ByteString(_))
      val fut = Source.apply(input).via(CsvParsing.lineScanner()).map(_.map(_.utf8String)).runWith(Sink.seq)
      val res = fut.futureValue
      res.head should be(List("eins", "zwei", "drei"))
      res(1) should be(List("uno", "dos", "tres"))
    }

    "emit completion even without new line at end" in assertAllStagesStopped {
      val (source, sink) = TestSource
        .probe[ByteString]
        .via(CsvParsing.lineScanner())
        .map(_.map(_.utf8String))
        .toMat(TestSink.probe[List[String]])(Keep.both)
        .run()
      source.sendNext(ByteString("eins,zwei,drei\nuno,dos,tres\n1,2,3"))
      sink.request(3)
      sink.expectNext(List("eins", "zwei", "drei"))
      sink.expectNext(List("uno", "dos", "tres"))
      sink.expectNoMessage(100.millis)
      source.sendComplete()
      sink.expectNext(List("1", "2", "3"))
      sink.expectComplete()
    }

    "read all lines without final line end and last column empty" in {
      val result = Source
        .single(ByteString("""eins,zwei,drei
          |uno,""".stripMargin))
        .via(CsvParsing.lineScanner())
        .map(_.map(_.utf8String))
        .runWith(Sink.seq)
        .futureValue

      result should have size 2
      result.head should be(List("eins", "zwei", "drei"))
      result(1) should be(List("uno", ""))
    }

    "parse Apple Numbers exported file" in assertAllStagesStopped {
      val fut =
        FileIO
          .fromPath(Paths.get("csv/src/test/resources/numbers-utf-8.csv"))
          .via(CsvParsing.lineScanner(delimiter = CsvParsing.SemiColon, escapeChar = '\u0001'))
          .map(_.map(_.utf8String))
          .runWith(Sink.seq)
      val res = fut.futureValue
      res(0) should be(List("abc", "def", "ghi", "", "", "", ""))
      res(1) should be(List("\"", "\\\\;", "a\"\nb\"\"c", "", "", "", ""))
    }

    "parse Google Docs exported file" in assertAllStagesStopped {
      val fut =
        FileIO
          .fromPath(Paths.get("csv/src/test/resources/google-docs.csv"))
          .via(CsvParsing.lineScanner(escapeChar = '\u0001'))
          .map(_.map(_.utf8String))
          .runWith(Sink.seq)
      val res = fut.futureValue
      res(0) should be(List("abc", "def", "ghi"))
      res(1) should be(List("\"", "\\\\,", "a\"\nb\"\"c"))
    }

    "parse uniVocity correctness test" in assertAllStagesStopped { // see https://github.com/uniVocity/csv-parsers-comparison
      val fut =
        FileIO
          .fromPath(Paths.get("csv/src/test/resources/correctness.csv"))
          .via(CsvParsing.lineScanner())
          .via(CsvToMap.toMap())
          .map(_.view.mapValues(_.utf8String).toIndexedSeq)
          .runWith(Sink.seq)
      val res = fut.futureValue
      res(0) should contain allElementsOf (
        Map(
          "Year" -> "1997",
          "Make" -> "Ford",
          "Model" -> "E350",
          "Description" -> "ac, abs, moon",
          "Price" -> "3000.00"
        )
      )
      res(1) should contain allElementsOf (
        Map(
          "Year" -> "1999",
          "Make" -> "Chevy",
          "Model" -> "Venture \"Extended Edition\"",
          "Description" -> "",
          "Price" -> "4900.00"
        )
      )
      res(2) should contain allElementsOf (
        Map(
          "Year" -> "1996",
          "Make" -> "Jeep",
          "Model" -> "Grand Cherokee",
          "Description" -> """MUST SELL!
                            |air, moon roof, loaded""".stripMargin,
          "Price" -> "4799.00"
        )
      )
      res(3) should contain allElementsOf (
        Map(
          "Year" -> "1999",
          "Make" -> "Chevy",
          "Model" -> "Venture \"Extended Edition, Very Large\"",
          "Description" -> "",
          "Price" -> "5000.00"
        )
      )
      res(4) should contain allElementsOf (
        Map(
          "Year" -> "",
          "Make" -> "",
          "Model" -> "Venture \"Extended Edition\"",
          "Description" -> "",
          "Price" -> "4900.00"
        )
      )
      res(5) should contain allElementsOf (
        Map(
          "Year" -> "1995",
          "Make" -> "VW",
          "Model" -> "Golf \"GTE\"",
          "Description" -> "",
          "Price" -> "5000.00"
        )
      )
      res(6) should contain allElementsOf (
        Map(
          "Year" -> "1996",
          "Make" -> "VW",
          "Model" -> "Golf GTE",
          "Description" -> "",
          "Price" -> "5000.00"
        )
      )
    }
  }
}
