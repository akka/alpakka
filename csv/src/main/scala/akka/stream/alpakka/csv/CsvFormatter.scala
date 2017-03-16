package akka.stream.alpakka.csv

import java.nio.charset.StandardCharsets

import akka.util.ByteString

import scala.annotation.switch
import scala.collection.immutable

/**
 * Internal API
 */
private[csv] class CsvFormatter(delimiter: Char,
                                quoteChar: Char,
                                escapeChar: Char,
                                endOfLine: ByteString,
                                quotingStyle: CsvQuotingStyle) {

  private[this] val charsetName = StandardCharsets.UTF_8.name()

  def toCsv(fields: immutable.Seq[Any]): ByteString =
    if (fields.nonEmpty) nonEmptyToCsv(fields) else endOfLine

  private def nonEmptyToCsv(fields: immutable.Seq[Any]) = {
    val builder = ByteString.createBuilder

    def splitAndDuplicateQuotesAndEscapes(field: String, splitAt: Int) = {
      val duplicatedQuote = ByteString(String.valueOf(Array(quoteChar, quoteChar)), charsetName)
      val duplicatedEscape = ByteString(String.valueOf(Array(escapeChar, escapeChar)), charsetName)

      @inline def indexOfQuoteOrEscape(lastIndex: Int) = {
        var index = lastIndex
        var found = -1
        while (index < field.length && found == -1) {
          val char = field(index)
          if (char == quoteChar || char == escapeChar) found = index
          index += 1
        }
        found
      }

      var lastIndex = 0
      var index = splitAt
      while (index > -1) {
        builder ++= ByteString.apply(field.substring(lastIndex, index), charsetName)
        val at = field.charAt(index)
        if (at == quoteChar) {
          builder ++= duplicatedQuote
        } else {
          builder ++= duplicatedEscape
        }
        lastIndex = index + 1
        index = indexOfQuoteOrEscape(lastIndex)
      }
      if (lastIndex < field.length) {
        builder ++= ByteString(field.substring(lastIndex), charsetName)
      }
    }

    def append(field: String) = {
      val (quoteIt, splitIt) = requiresQuotesOrSplit(field)
      if (quoteIt || quotingStyle == CsvQuotingStyle.ALWAYS) {
        val quoteByteString = ByteString(String.valueOf(quoteChar), charsetName)
        builder ++= quoteByteString
        if (splitIt != -1) {
          splitAndDuplicateQuotesAndEscapes(field, splitIt)
        } else {
          builder ++= ByteString(field, charsetName)
        }
        builder ++= quoteByteString
      } else {
        builder ++= ByteString(field, charsetName)
      }
    }

    val delimiterBs = ByteString(String.valueOf(delimiter), charsetName)
    val iterator = fields.iterator
    var hasNext = iterator.hasNext
    while (hasNext) {
      val next = iterator.next()
      if (next != null) {
        append(next.toString)
      }
      hasNext = iterator.hasNext
      if (hasNext) {
        builder ++= delimiterBs
      }
    }
    builder ++= endOfLine
    builder.result()
  }

  private def requiresQuotesOrSplit(field: String): (Boolean, Int) = {
    var quotes = false
    var split = -1
    var index = 0
    while (index < field.length && !(quotes && split != -1)) {
      val char = field(index)
      if (char == `quoteChar` || char == `escapeChar`) {
        quotes = true
        split = index
      } else if (char == '\r' || char == '\n' || char == `delimiter`) {
        quotes = true
      }
      index += 1
    }
    (quotes, split)
  }
}
