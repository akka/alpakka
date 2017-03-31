/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.csv.scaladsl

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Sink, Source}
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
    val flow: Flow[immutable.Seq[String], ByteString, NotUsed]
      = CsvFormatting.format(delimiter,
                             quoteChar,
                             escapeChar,
                             endOfLine,
                             CsvQuotingStyle.Required,
                             charsetName = "UTF-8")
    // #flow-type
    // format: on
  }

  "CSV Formatting" should {
    "format simple value" in {
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

  }
}
