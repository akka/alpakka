/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.csv.impl

import java.nio.charset.UnsupportedCharsetException

import akka.annotation.InternalApi
import akka.stream.alpakka.csv.MalformedCsvException
import akka.stream.alpakka.csv.scaladsl.ByteOrderMark
import akka.util.{ByteIterator, ByteString, ByteStringBuilder}

import scala.collection.mutable

/**
 * INTERNAL API: Use [[akka.stream.alpakka.csv.scaladsl.CsvParsing]] instead.
 */
@InternalApi private[csv] object CsvParser {

  private type State = Int
  private final val LineStart = 0
  private final val WithinField = 1
  private final val WithinFieldEscaped = 2
  private final val AfterDelimiter = 3
  private final val LineEnd = 4
  private final val QuoteStarted = 5
  private final val WithinQuotedField = 6
  private final val WithinQuotedFieldEscaped = 7
  private final val WithinQuotedFieldQuote = 8
  private final val AfterCr = 9

  private final val LF: Byte = '\n'
  private final val CR: Byte = '\r'
}

/**
 * INTERNAL API: Use [[akka.stream.alpakka.csv.scaladsl.CsvParsing]] instead.
 */
@InternalApi private[csv] final class CsvParser(delimiter: Byte,
                                                quoteChar: Byte,
                                                escapeChar: Byte,
                                                maximumLineLength: Int) {

  import CsvParser._

  /**
   * Concatenated input chunks,
   * appended to by [[offer()]] and dropped from by [[dropReadBuffer()]].
   *
   * May include previous chunks that start a field but do not complete it.
   */
  private[this] var buffer: ByteString = ByteString.empty

  /**
   * Flag to run BOM checks against first two bytes of the stream.
   */
  private[this] var firstData = true

  /**
   * Current position within [[buffer]].
   *
   * Points to the same byte as [[current.head]].
   * Used for slicing fields out of [[buffer]] and for debug info.
   */
  private[this] var pos: Int = 0

  /**
   * Number of bytes dropped on the current row.
   *
   * Perf:
   * We need to track this in order to call [[dropReadBuffer()]] after each field instead of each line.
   * We want to call [[dropReadBuffer()]] ASAP to convert [[buffer]] from a
   * [[akka.util.ByteString.ByteStrings]] to a [[akka.util.ByteString.ByteString1]]
   * to exploit the much faster [[ByteString.slice()]] implementation.
   */
  private[this] var lineBytesDropped = 0

  /**
   * Position within the current row.
   *
   * Used for enforcing line length limits and as debug info for exceptions.
   */
  private[this] def lineLength: Int = lineBytesDropped + pos

  /**
   * Position within [[buffer]] of the start of the current field.
   */
  private[this] var fieldStart = 0
  private[this] var currentLineNo = 1L

  /**
   * Reset after each row.
   */
  private[this] var columns = mutable.ListBuffer[ByteString]()
  private[this] var state: State = LineStart
  private[this] var fieldBuilder = new FieldBuilder

  /**
   * Current iterator being parsed.
   *
   * Previous implementation indexed into [[buffer.apply()]] for each byte,
   * which is slow against [[akka.util.ByteString.ByteStrings]].
   *
   * We fully parse each chunk before getting the next, so we only need to track one [[ByteIterator]] at a time.
   */
  private[this] var current: ByteIterator = ByteString.empty.iterator

  def offer(next: ByteString): Unit =
    if (next.nonEmpty) {
      require(current.isEmpty, "offer(ByteString) may not be called before all buffered input is parsed.")
      buffer ++= next
      current = next.iterator
    }

  def poll(requireLineEnd: Boolean): Option[List[ByteString]] = {
    if (buffer.nonEmpty) parseLine()
    val line = maybeExtractLine(requireLineEnd)
    if (line.nonEmpty) {
      currentLineNo += 1
      if (state == LineEnd || !requireLineEnd) {
        state = LineStart
      }
      resetLine()
      columns.clear()
    }
    line
  }

  private[this] def advance(n: Int = 1): Unit = {
    pos += n
    current.drop(n)
  }

  private[this] def resetLine(): Unit = {
    dropReadBuffer()
    lineBytesDropped = 0
  }

  private[this] def dropReadBuffer() = {
    buffer = buffer.drop(pos)
    lineBytesDropped += pos
    pos = 0
    fieldStart = 0
  }

  /** FieldBuilder will just cut the required part out of the incoming ByteBuffer
   * as long as non escaping is used.
   */
  private final class FieldBuilder {

    /**
     * false if [[builder]] is null.
     */
    private[this] var useBuilder = false
    private[this] var builder: ByteStringBuilder = _

    /** Set up the ByteString builder instead of relying on `ByteString.slice`.
     */
    @inline def init(): Unit =
      if (!useBuilder) {
        builder = ByteString.newBuilder ++= buffer.slice(fieldStart, pos)
        useBuilder = true
      }

    @inline def add(x: Byte): Unit =
      if (useBuilder) builder += x

    @inline def result(pos: Int): ByteString =
      if (useBuilder) {
        useBuilder = false
        builder.result()
      } else buffer.slice(fieldStart, pos)

  }

  private[this] def noCharEscaped() =
    throw new MalformedCsvException(currentLineNo,
                                    lineLength,
                                    s"wrong escaping at $currentLineNo:$lineLength, no character after escape")

  private[this] def checkForByteOrderMark(): Unit =
    if (buffer.length >= 2) {
      if (buffer.startsWith(ByteOrderMark.UTF_8)) {
        advance(4)
        fieldStart = 3
      } else {
        if (buffer.startsWith(ByteOrderMark.UTF_16_LE)) {
          throw new UnsupportedCharsetException("UTF-16 LE and UTF-32 LE")
        }
        if (buffer.startsWith(ByteOrderMark.UTF_16_BE)) {
          throw new UnsupportedCharsetException("UTF-16 BE")
        }
        if (buffer.startsWith(ByteOrderMark.UTF_32_BE)) {
          throw new UnsupportedCharsetException("UTF-32 BE")
        }
      }
    }

  private[this] def parseLine(): Unit = {
    if (firstData) {
      checkForByteOrderMark()
      firstData = false
    }
    churn()
  }

  private[this] def churn(): Unit = {
    while (state != LineEnd && pos < buffer.length) {
      if (lineLength >= maximumLineLength)
        throw new MalformedCsvException(
          currentLineNo,
          lineLength,
          s"no line end encountered within $maximumLineLength bytes on line $currentLineNo"
        )
      val byte = current.head
      state match {
        case LineStart =>
          byte match {
            case `quoteChar` =>
              state = QuoteStarted
              advance()
              fieldStart = pos
            case `escapeChar` =>
              fieldBuilder.init()
              state = WithinFieldEscaped
              advance()
              fieldStart = pos
            case `delimiter` =>
              columns += ByteString.empty
              state = AfterDelimiter
              advance()
              fieldStart = pos
            case LF =>
              columns += ByteString.empty
              state = LineEnd
              advance()
              fieldStart = pos
            case CR =>
              columns += ByteString.empty
              state = AfterCr
              advance()
              fieldStart = pos
            case b =>
              fieldBuilder.add(b)
              state = WithinField
              advance()
          }

        case AfterDelimiter =>
          byte match {
            case `quoteChar` =>
              state = QuoteStarted
              advance()
              fieldStart = pos
            case `escapeChar` =>
              fieldBuilder.init()
              state = WithinFieldEscaped
              advance()
              fieldStart = pos
            case `delimiter` =>
              columns += ByteString.empty
              state = AfterDelimiter
              advance()
              fieldStart = pos
            case LF =>
              columns += ByteString.empty
              state = LineEnd
              advance()
              fieldStart = pos
            case CR =>
              columns += ByteString.empty
              state = AfterCr
              advance()
              fieldStart = pos
            case b =>
              fieldBuilder.add(b)
              state = WithinField
              advance()
          }

        case WithinField =>
          byte match {
            case `escapeChar` =>
              fieldBuilder.init()
              state = WithinFieldEscaped
              advance()
            case `delimiter` =>
              columns += fieldBuilder.result(pos)
              state = AfterDelimiter
              advance()
              dropReadBuffer()
            case LF =>
              columns += fieldBuilder.result(pos)
              state = LineEnd
              advance()
              dropReadBuffer()
            case CR =>
              columns += fieldBuilder.result(pos)
              state = AfterCr
              advance()
              dropReadBuffer()
            case b =>
              fieldBuilder.add(b)
              state = WithinField
              advance()
          }

        case WithinFieldEscaped =>
          byte match {
            case `escapeChar` | `delimiter` =>
              fieldBuilder.add(byte)
              state = WithinField
              advance()

            case `quoteChar` =>
              throw new MalformedCsvException(
                currentLineNo,
                lineLength,
                s"wrong escaping at $currentLineNo:$lineLength, quote is escaped as ${quoteChar.toChar}${quoteChar.toChar}"
              )

            case b =>
              fieldBuilder.add(escapeChar)
              state = WithinField

          }

        case QuoteStarted =>
          byte match {
            case `escapeChar` if escapeChar != quoteChar =>
              fieldBuilder.init()
              state = WithinQuotedFieldEscaped
              advance()
            case `quoteChar` =>
              fieldBuilder.init()
              state = WithinQuotedFieldQuote
              advance()
            case b =>
              fieldBuilder.add(b)
              state = WithinQuotedField
              advance()
          }

        case WithinQuotedField =>
          byte match {
            case `escapeChar` if escapeChar != quoteChar =>
              fieldBuilder.init()
              state = WithinQuotedFieldEscaped
              advance()
            case `quoteChar` =>
              fieldBuilder.init()
              state = WithinQuotedFieldQuote
              advance()
            case b =>
              fieldBuilder.add(b)
              state = WithinQuotedField
              advance()
          }

        case WithinQuotedFieldEscaped =>
          byte match {
            case `escapeChar` | `quoteChar` =>
              fieldBuilder.add(byte)
              state = WithinQuotedField
              advance()

            case b =>
              fieldBuilder.add(escapeChar)
              state = WithinQuotedField
          }

        case WithinQuotedFieldQuote =>
          byte match {
            case `quoteChar` =>
              fieldBuilder.add(byte)
              state = WithinQuotedField
              advance()

            case b =>
              state = WithinField
          }

        case AfterCr =>
          byte match {
            case CR =>
              state = AfterCr
              advance()
            case LF =>
              state = LineEnd
              advance()
            case _ =>
              state = LineEnd
          }
      }
    }
  }
  private[this] def maybeExtractLine(requireLineEnd: Boolean): Option[List[ByteString]] =
    if (requireLineEnd) {
      state match {
        case LineEnd =>
          Some(columns.toList)
        case _ =>
          None
      }
    } else {
      state match {
        case AfterDelimiter =>
          columns += ByteString.empty
          Some(columns.toList)
        case WithinQuotedField =>
          throw new MalformedCsvException(
            currentLineNo,
            lineLength,
            s"unclosed quote at end of input $currentLineNo:$lineLength, no matching quote found"
          )
        case WithinField =>
          columns += fieldBuilder.result(pos)
          Some(columns.toList)
        case WithinQuotedFieldQuote =>
          columns += fieldBuilder.result(pos - 1)
          Some(columns.toList)
        case WithinFieldEscaped | WithinQuotedFieldEscaped =>
          noCharEscaped()
        case _ if columns.nonEmpty =>
          Some(columns.toList)
        case _ =>
          None
      }
    }

}
