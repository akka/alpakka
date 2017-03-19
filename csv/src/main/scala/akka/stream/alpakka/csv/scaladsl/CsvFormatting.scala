/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.csv.scaladsl

import akka.NotUsed
import akka.stream.alpakka.csv.{javadsl, CsvFormattingStage}
import akka.stream.scaladsl.Flow
import akka.util.ByteString

import scala.collection.immutable

sealed trait CsvQuotingStyle
object CsvQuotingStyle {
  case object Required extends CsvQuotingStyle
  case object Always extends CsvQuotingStyle

  def asScala(qs: javadsl.CsvQuotingStyle): CsvQuotingStyle = qs match {
    case javadsl.CsvQuotingStyle.ALWAYS => CsvQuotingStyle.Always
    case javadsl.CsvQuotingStyle.REQUIRED => CsvQuotingStyle.Required
  }

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
                                              quotingStyle: CsvQuotingStyle = CsvQuotingStyle.Required,
                                              charsetName: String = ByteString.UTF_8): Flow[T, ByteString, NotUsed] =
    Flow[immutable.Iterable[String]]
      .via(new CsvFormattingStage(delimiter, quoteChar, escapeChar, endOfLine, quotingStyle, charsetName))
}
