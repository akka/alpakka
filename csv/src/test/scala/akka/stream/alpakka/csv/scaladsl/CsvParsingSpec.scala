/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.csv.scaladsl

import java.nio.charset.StandardCharsets
import java.nio.file.Paths

import akka.NotUsed
import akka.stream.scaladsl.{FileIO, Flow, Sink, Source}
import akka.util.ByteString

import scala.collection.immutable.Seq

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

    "parse Apple Numbers exported file" in {
      val fut =
        FileIO
          .fromPath(Paths.get("csv/src/test/resources/numbers-utf-8.csv"))
          .via(CsvParsing.lineScanner(delimiter = CsvParsing.SemiColon, escapeChar = '\u0001'))
          .map(_.map(_.utf8String))
          .runWith(Sink.seq)
      val res = fut.futureValue
      res(0) should be(List("abc", "def", "ghi", "", "", "", ""))
      res(1) should be(List("\"", "\\\\;", "a\"\nbc", "", "", "", ""))
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
      res(1) should be(List("\"", "\\\\,", "a\"\nbc"))
    }
  }
}
