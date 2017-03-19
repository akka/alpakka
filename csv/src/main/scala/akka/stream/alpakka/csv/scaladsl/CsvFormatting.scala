/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.csv.scaladsl

import akka.NotUsed
import akka.stream.alpakka.csv.{CsvFormattingStage, CsvQuotingStyle}
import akka.stream.scaladsl.Flow
import akka.util.ByteString

import scala.collection.immutable

sealed trait CsvQuoting
object CsvQuoting {
  case object Required extends CsvQuoting
  case object Always extends CsvQuoting
}

/** Provides CSV formatting flows that convert a sequence of String into their CSV representation
  * in [[akka.util.ByteString]].
  */
object CsvFormatting {

  val Backslash: Char = '\\'
  val Comma: Char = ','
  val SemiColon: Char = ';'
  val Colon: Char = ':'
  val Tab: Char = '\t'
  val DoubleQuote: Char = '"'

  def format[T <: immutable.Iterable[String]](delimiter: Char = Comma,
                                              quoteChar: Char = DoubleQuote,
                                              escapeChar: Char = Backslash,
                                              endOfLine: String = "\r\n",
                                              quotingStyle: CsvQuoting = CsvQuoting.Required,
                                              charsetName: String = ByteString.UTF_8): Flow[T, ByteString, NotUsed] =
    Flow[immutable.Iterable[String]]
      .via(new CsvFormattingStage(delimiter, quoteChar, escapeChar, endOfLine, quotingStyle, charsetName))

  def format[T <: immutable.Iterable[String]](delimiter: Char,
                                              quoteChar: Char,
                                              escapeChar: Char,
                                              endOfLine: String,
                                              quotingStyle: CsvQuotingStyle,
                                              charsetName: String): Flow[T, ByteString, NotUsed] = {
    val qs = quotingStyle match {
      case CsvQuotingStyle.ALWAYS => CsvQuoting.Always
      case CsvQuotingStyle.REQUIRED => CsvQuoting.Required
    }
    Flow[immutable.Iterable[String]]
      .via(new CsvFormattingStage(delimiter, quoteChar, escapeChar, endOfLine, qs, charsetName))
  }

}
