/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.csv.scaladsl

import akka.NotUsed
import akka.stream.alpakka.csv.{javadsl, CsvFormatter}
import akka.stream.scaladsl.{Flow, Source}
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

  /**
    *
    * @param charsetName
    * @param byteOrderMark Certain CSV readers (namely Microsoft Excel) require a
    */
  def format[T <: immutable.Iterable[String]](
      delimiter: Char = Comma,
      quoteChar: Char = DoubleQuote,
      escapeChar: Char = Backslash,
      endOfLine: String = "\r\n",
      quotingStyle: CsvQuotingStyle = CsvQuotingStyle.Required,
      charsetName: String = ByteString.UTF_8,
      byteOrderMark: Option[ByteString] = None
  ): Flow[T, ByteString, NotUsed] = {
    val formatter =
      new CsvFormatter(delimiter, quoteChar, escapeChar, endOfLine, quotingStyle, charsetName)
    byteOrderMark.fold {
      Flow[T].map { t =>
        formatter.toCsv(t)
      }.named("CsvFormatting")
    } { bom =>
      Flow[T]
        .map { t =>
          formatter.toCsv(t)
        }.named("CsvFormatting")
        .prepend(Source.single(bom))
    }

  }
}
