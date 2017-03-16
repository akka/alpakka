/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.csv.scaladsl

import akka.NotUsed
import akka.stream.alpakka.csv.{CsvFormattingStage, CsvQuotingStyle}
import akka.stream.scaladsl.Flow
import akka.util.ByteString

import scala.collection.immutable

/** Provides CSV formatting stages that convert a sequence of String into their CSV representation in [[akka.util.ByteString]]. */
object CsvFormatting {

  val Backslash: Char = '\\'
  val Comma: Char = ','
  val SemiColon: Char = ';'
  val Colon: Char = ':'
  val DoubleQuote: Char = '"'
  val Tab: Char = '\t'

  def format(delimiter: Char = Comma,
             quoteChar: Char = DoubleQuote,
             escapeChar: Char = Backslash,
             endOfLine: String = "\r\n",
             quotingStyle: CsvQuotingStyle = CsvQuotingStyle.REQUIRED,
             charsetName: String = ByteString.UTF_8): Flow[immutable.Seq[String], ByteString, NotUsed] =
    Flow[immutable.Seq[String]]
      .via(new CsvFormattingStage(delimiter, quoteChar, escapeChar, endOfLine, quotingStyle, charsetName))
}
