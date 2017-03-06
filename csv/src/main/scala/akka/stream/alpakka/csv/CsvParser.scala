/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.csv

import akka.util.{ByteString, ByteStringBuilder}

object CsvParser {

  class MalformedCSVException(position: Int, msg: String) extends Exception

  private type State = Int
  private final val LineStart = 0
  private final val WithinField = 1
  private final val AfterDelimiter = 2
  private final val End = 3
  private final val QuoteStarted = 4
  private final val QuoteEnd = 5
  private final val WithinQuotedField = 6
}

class CsvParser(escapeChar: Byte = '\\', delimiter: Byte = ',', quoteChar: Byte = '"') {
  import CsvParser._

  private var buffer = ByteString.empty
  private var pos = 0
  private var fieldStart = 0

  def offer(input: ByteString) =
    buffer ++= input

  def poll(): Option[List[ByteString]] =
    if (buffer.nonEmpty) {
      val line = parseLine()
      if (line.nonEmpty) {
        dropReadBuffer()
      }
      line
    } else None

  private def dropReadBuffer() = {
    buffer = buffer.drop(pos)
    pos = 0
    fieldStart = 0
  }

  /** FieldBuilder will just cut the required part out of the incoming ByteBuffer
   * as long as non escaping is used.
   */
  protected class FieldBuilder(buf: ByteString) {

    private var useBuilder = false
    private var builder: ByteStringBuilder = null

    /** Set up the ByteString builder instead of relying on `ByteString.slice`.
     */
    @inline def init(x: Byte): Unit =
      if (!useBuilder) {
        builder = ByteString.newBuilder ++= buf.slice(fieldStart, pos) += x
        useBuilder = true
      }

    @inline def add(x: Byte): Unit =
      if (useBuilder) builder += x

    @inline def result(pos: Int): ByteString =
      if (useBuilder) {
        useBuilder = false
        builder.result()
      } else buf.slice(fieldStart, pos)

  }

  protected def parseLine(): Option[List[ByteString]] = {
    val buf = buffer
    var columns = Vector[ByteString]()
    var state: State = LineStart
    val fieldBuilder = new FieldBuilder(buf)

    def wrongCharEscaped() =
      throw new MalformedCSVException(pos, s"wrong escaping at $pos, only escape or delimiter may be escaped")
    def wrongCharEscapedWithinQuotes() =
      throw new MalformedCSVException(pos,
        s"wrong escaping at $pos, only escape or quote may be escaped within quotes")
    def noCharEscaped() = throw new MalformedCSVException(pos, s"wrong escaping at $pos, no character after escape")

    @inline def readPastLf() =
      if (pos < buf.length && buf(pos) == '\n') {
        pos += 1
      }

    while (state != End && pos < buf.length) {
      val byte = buf(pos)
      state match {
        case LineStart =>
          byte match {
            case `quoteChar` =>
              state = QuoteStarted
              pos += 1
              fieldStart = pos
            case `delimiter` =>
              columns :+= ByteString.empty
              state = AfterDelimiter
              pos += 1
              fieldStart = pos
            case '\n' => //| '\u2028' | '\u2029' | '\u0085' => {
              columns :+= ByteString.empty
              state = End
              pos += 1
              fieldStart = pos
            case '\r' =>
              columns :+= ByteString.empty
              state = End
              pos += 1
              readPastLf()
              fieldStart = pos
            case x =>
              fieldBuilder.add(x)
              state = WithinField
              pos += 1
          }

        case AfterDelimiter =>
          byte match {
            case `quoteChar` =>
              state = QuoteStarted
              pos += 1
              fieldStart = pos
            case `escapeChar` =>
              if (pos + 1 < buf.length) {
                if (buf(pos + 1) == escapeChar || buf(pos + 1) == delimiter) {
                  fieldBuilder.init(buf(pos + 1))
                  state = WithinField
                  pos += 2
                } else wrongCharEscaped()
              } else noCharEscaped()
            case `delimiter` =>
              columns :+= ByteString.empty
              state = AfterDelimiter
              pos += 1
              fieldStart = pos
            case '\n' => // | '\u2028' | '\u2029' | '\u0085' =>
              columns :+= ByteString.empty
              state = End
              pos += 1
              fieldStart = pos
            case '\r' =>
              columns :+= ByteString.empty
              state = End
              pos += 1
              readPastLf()
              fieldStart = pos
            case x =>
              fieldBuilder.add(x)
              state = WithinField
              pos += 1
          }

        case WithinField =>
          byte match {
            case `escapeChar` =>
              if (pos + 1 < buf.length) {
                if (buf(pos + 1) == escapeChar || buf(pos + 1) == delimiter) {
                  fieldBuilder.add(buf(pos + 1))
                  state = WithinField
                  pos += 2
                } else wrongCharEscaped()
              } else noCharEscaped()
            case `delimiter` =>
              columns :+= fieldBuilder.result(pos)
              state = AfterDelimiter
              pos += 1
              fieldStart = pos
            case '\n' => //| '\u2028' | '\u2029' | '\u0085' => {
              columns :+= fieldBuilder.result(pos)
              state = End
              pos += 1
              fieldStart = pos
            case '\r' =>
              columns :+= fieldBuilder.result(pos)
              state = End
              pos += 1
              readPastLf()
              fieldStart = pos
            case x =>
              fieldBuilder.add(x)
              state = WithinField
              pos += 1
          }

        case QuoteStarted =>
          byte match {
            case `escapeChar` if escapeChar != quoteChar =>
              if (pos + 1 < buf.length) {
                if (buf(pos + 1) == escapeChar || buf(pos + 1) == quoteChar) {
                  fieldBuilder.init(buf(pos + 1))
                  state = WithinQuotedField
                  pos += 2
                } else wrongCharEscapedWithinQuotes()
              } else noCharEscaped()
            case `quoteChar` =>
              if (pos + 1 < buf.length && buf(pos + 1) == quoteChar) {
                fieldBuilder.init(byte)
                state = WithinQuotedField
                pos += 2
              } else {
                state = QuoteEnd
                pos += 1
              }
            case x =>
              fieldBuilder.add(x)
              state = WithinQuotedField
              pos += 1
          }

        case QuoteEnd =>
          byte match {
            case `delimiter` =>
              columns :+= fieldBuilder.result(pos - 1)
              state = AfterDelimiter
              pos += 1
              fieldStart = pos
            case '\n' => //| '\u2028' | '\u2029' | '\u0085' =>
              columns :+= fieldBuilder.result(pos - 1)
              state = End
              pos += 1
              fieldStart = pos
            case '\r' =>
              columns :+= fieldBuilder.result(pos - 1)
              state = End
              pos += 1
              readPastLf()
              fieldStart = pos
            case _ =>
              throw new MalformedCSVException(pos, "expected delimiter or end of line")
          }

        case WithinQuotedField =>
          byte match {
            case `escapeChar` if escapeChar != quoteChar =>
              if (pos + 1 < buf.length) {
                if (buf(pos + 1) == escapeChar || buf(pos + 1) == quoteChar) {
                  fieldBuilder.init(buf(pos + 1))
                  state = WithinQuotedField
                  pos += 2
                } else wrongCharEscapedWithinQuotes()
              } else noCharEscaped()

            case `quoteChar` =>
              if (pos + 1 < buf.length && buf(pos + 1) == quoteChar) {
                fieldBuilder.init(byte)
                state = WithinQuotedField
                pos += 2
              } else {
                state = QuoteEnd
                pos += 1
              }
            case x =>
              fieldBuilder.add(x)
              state = WithinQuotedField
              pos += 1
          }

        case End =>
          sys.error("unexpected error")
      }
    }
    state match {
      case AfterDelimiter =>
        columns :+= ByteString.empty
        Some(columns.toList)

      case WithinQuotedField =>
        None

      case _ =>
        state match {
          case WithinField =>
            columns :+= fieldBuilder.result(pos)
          case QuoteEnd =>
            columns :+= fieldBuilder.result(pos - 1)
          case _ =>
        }
        Some(columns.toList)

    }

  }

}
