/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.csv

import akka.util.{ByteString, ByteStringBuilder}

/**
 * INTERNAL API: Use [[akka.stream.alpakka.csv.scaladsl.CsvFraming]] instead.
 */
private[csv] object CsvParser {

  class MalformedCsvException(position: Int, msg: String) extends Exception

  private type State = Int
  private final val LineStart = 0
  private final val WithinField = 1
  private final val AfterDelimiter = 2
  private final val LineEnd = 3
  private final val QuoteStarted = 4
  private final val QuoteEnd = 5
  private final val WithinQuotedField = 6

  private final val LF: Byte = '\n'
  private final val CR: Byte = '\r'
}

/**
 * INTERNAL API: Use [[akka.stream.alpakka.csv.scaladsl.CsvFraming]] instead.
 */
private[csv] final class CsvParser(delimiter: Byte, quoteChar: Byte, escapeChar: Byte) {
  import CsvParser._

  private[this] var buffer = ByteString.empty
  private[this] var pos = 0
  private[this] var fieldStart = 0

  def offer(input: ByteString): Unit =
    buffer ++= input

  def poll(requireLineEnd: Boolean): Option[List[ByteString]] =
    if (buffer.nonEmpty) {
      val prePos = pos
      val preFieldStart = fieldStart
      val line = parseLine(requireLineEnd)
      if (line.nonEmpty) {
        dropReadBuffer()
      } else {
        pos = prePos
        fieldStart = preFieldStart
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
  private final class FieldBuilder(buf: ByteString) {

    private[this] var useBuilder = false
    private[this] var builder: ByteStringBuilder = _

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

  protected def parseLine(requireLineEnd: Boolean): Option[List[ByteString]] = {
    val buf = buffer
    var columns = Vector[ByteString]()
    var state: State = LineStart
    val fieldBuilder = new FieldBuilder(buf)

    def wrongCharEscaped() =
      throw new MalformedCsvException(pos, s"wrong escaping at $pos, only escape or delimiter may be escaped")
    def wrongCharEscapedWithinQuotes() =
      throw new MalformedCsvException(pos,
        s"wrong escaping at $pos, only escape or quote may be escaped within quotes")
    def noCharEscaped() = throw new MalformedCsvException(pos, s"wrong escaping at $pos, no character after escape")

    @inline def readPastLf() =
      if (pos < buf.length && buf(pos) == LF) {
        pos += 1
      }

    while (state != LineEnd && pos < buf.length) {
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
            case LF =>
              columns :+= ByteString.empty
              state = LineEnd
              pos += 1
              fieldStart = pos
            case CR =>
              columns :+= ByteString.empty
              state = LineEnd
              pos += 1
              readPastLf()
              fieldStart = pos
            case b =>
              fieldBuilder.add(b)
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
            case LF =>
              columns :+= ByteString.empty
              state = LineEnd
              pos += 1
              fieldStart = pos
            case CR =>
              columns :+= ByteString.empty
              state = LineEnd
              pos += 1
              readPastLf()
              fieldStart = pos
            case b =>
              fieldBuilder.add(b)
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
            case LF =>
              columns :+= fieldBuilder.result(pos)
              state = LineEnd
              pos += 1
              fieldStart = pos
            case CR =>
              columns :+= fieldBuilder.result(pos)
              state = LineEnd
              pos += 1
              readPastLf()
              fieldStart = pos
            case b =>
              fieldBuilder.add(b)
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
            case b =>
              fieldBuilder.add(b)
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
            case LF =>
              columns :+= fieldBuilder.result(pos - 1)
              state = LineEnd
              pos += 1
              fieldStart = pos
            case CR =>
              columns :+= fieldBuilder.result(pos - 1)
              state = LineEnd
              pos += 1
              readPastLf()
              fieldStart = pos
            case _ =>
              throw new MalformedCsvException(pos, "expected delimiter or end of line")
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
            case b =>
              fieldBuilder.add(b)
              state = WithinQuotedField
              pos += 1
          }
      }
    }
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
          columns :+= ByteString.empty
          Some(columns.toList)
        case WithinQuotedField =>
          None
        case WithinField =>
          columns :+= fieldBuilder.result(pos)
          Some(columns.toList)
        case QuoteEnd =>
          columns :+= fieldBuilder.result(pos - 1)
          Some(columns.toList)
        case _ =>
          Some(columns.toList)
      }
    }
  }

}
