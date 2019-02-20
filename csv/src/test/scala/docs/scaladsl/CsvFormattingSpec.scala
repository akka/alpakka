/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.nio.charset.StandardCharsets

import akka.stream.alpakka.csv.scaladsl.{ByteOrderMark, CsvFormatting, CsvQuotingStyle}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.util.ByteString

import scala.collection.immutable

class CsvFormattingSpec extends CsvSpec {

  def documentation(): Unit = {
    import CsvFormatting._
    val delimiter = Comma
    val quoteChar = DoubleQuote
    val escapeChar = Backslash
    val endOfLine = "\r\n"
    // format: off
    // #flow-type
    val flow: Flow[immutable.Seq[String], ByteString, _]
      = CsvFormatting.format(delimiter,
                             quoteChar,
                             escapeChar,
                             endOfLine,
                             CsvQuotingStyle.Required,
                             charset = StandardCharsets.UTF_8,
                             byteOrderMark = None)
    // #flow-type
    // format: on
  }

  "CSV Formatting" should {
    "format simple value" in assertAllStagesStopped {
      // #formatting
      import akka.stream.alpakka.csv.scaladsl.CsvFormatting

      // #formatting
      val fut =
        // format: off
      // #formatting
      Source
        .single(List("eins", "zwei", "drei"))
        .via(CsvFormatting.format())
        .runWith(Sink.head)
      // #formatting
      // format: on
      fut.futureValue should be(ByteString("eins,zwei,drei\r\n"))
    }

    "include Byte Order Mark" in assertAllStagesStopped {
      // #formatting-bom
      import akka.stream.alpakka.csv.scaladsl.CsvFormatting

      // #formatting-bom
      val fut =
        // format: off
      // #formatting-bom
        Source
          .apply(List(List("eins", "zwei", "drei"), List("uno", "dos", "tres")))
          .via(CsvFormatting.format(byteOrderMark = Some(ByteOrderMark.UTF_8)))
          .runWith(Sink.seq)
      // #formatting-bom
      // format: on
      fut.futureValue should be(
        List(ByteOrderMark.UTF_8, ByteString("eins,zwei,drei\r\n"), ByteString("uno,dos,tres\r\n"))
      )
    }

  }
}
