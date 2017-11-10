/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.csv.scaladsl

import java.nio.file.Paths

import akka.NotUsed
import akka.stream.scaladsl.{FileIO, Flow, Keep, Sink, Source}
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
  }

  "CSV parsing" should {
    "parse one line" in {
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
      fut.futureValue should be(List(ByteString("eins"), ByteString("zwei"), ByteString("drei")))
    }

    "parse two lines" in {
      val fut =
        Source.single(ByteString("eins,zwei,drei\nuno,dos,tres\n")).via(CsvParsing.lineScanner()).runWith(Sink.seq)
      val res = fut.futureValue
      res.head should be(List(ByteString("eins"), ByteString("zwei"), ByteString("drei")))
      res(1) should be(List(ByteString("uno"), ByteString("dos"), ByteString("tres")))
    }

    "parse two lines even witouth line end" in {
      val fut =
        Source.single(ByteString("eins,zwei,drei\nuno,dos,tres")).via(CsvParsing.lineScanner()).runWith(Sink.seq)
      val res = fut.futureValue
      res.head should be(List(ByteString("eins"), ByteString("zwei"), ByteString("drei")))
      res(1) should be(List(ByteString("uno"), ByteString("dos"), ByteString("tres")))
    }

    "parse semicolon lines" in {
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

    "parse chunks successfully" in {
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

    "emit completion even without new line at end" in {
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
      sink.expectNoMsg(100.millis)
      source.sendComplete()
      sink.expectNext(List("1", "2", "3"))
      sink.expectComplete()
    }

    "parse Apple Numbers exported file" in {
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

    "parse Google Docs exported file" in {
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

    "parse uniVocity correctness test" in { // see https://github.com/uniVocity/csv-parsers-comparison
      val fut =
        FileIO
          .fromPath(Paths.get("csv/src/test/resources/correctness.csv"))
          .via(CsvParsing.lineScanner())
          .via(CsvToMap.toMap())
          .map(_.mapValues(_.utf8String))
          .runWith(Sink.seq)
      val res = fut.futureValue
      res(0) should be(
        Map(
          "Year" -> "1997",
          "Make" -> "Ford",
          "Model" -> "E350",
          "Description" -> "ac, abs, moon",
          "Price" -> "3000.00"
        )
      )
      res(1) should be(
        Map(
          "Year" -> "1999",
          "Make" -> "Chevy",
          "Model" -> "Venture \"Extended Edition\"",
          "Description" -> "",
          "Price" -> "4900.00"
        )
      )
      res(2) should be(
        Map(
          "Year" -> "1996",
          "Make" -> "Jeep",
          "Model" -> "Grand Cherokee",
          "Description" -> """MUST SELL!
                            |air, moon roof, loaded""".stripMargin,
          "Price" -> "4799.00"
        )
      )
      res(3) should be(
        Map(
          "Year" -> "1999",
          "Make" -> "Chevy",
          "Model" -> "Venture \"Extended Edition, Very Large\"",
          "Description" -> "",
          "Price" -> "5000.00"
        )
      )
      res(4) should be(
        Map(
          "Year" -> "",
          "Make" -> "",
          "Model" -> "Venture \"Extended Edition\"",
          "Description" -> "",
          "Price" -> "4900.00"
        )
      )
    }
  }
}
