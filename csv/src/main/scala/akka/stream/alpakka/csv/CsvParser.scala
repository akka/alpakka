package akka.stream.alpakka.csv

import akka.util.{ByteString, ByteStringBuilder}

import scala.annotation.switch

object CsvParser {

  class MalformedCSVException(position: Int, msg: String) extends Exception

  private type State = Int
  private final val LineStart = 0
  private final val WithinField = 1
  private final val AfterDelimiter = 2
  private final val End = 3
  private final val QuoteStarted = 4
  private final val QuoteEnd = 5
  private final val QuotedField = 6
}

class CsvParser(escapeChar: Byte = '\\', delimiter: Byte = ',', quoteChar: Byte = '"') {
  import CsvParser._

  private var buffer = ByteString.empty
  private var pos = 0
  private var fieldStart = 0

  def offer(input: ByteString) =
    buffer ++= input

  def poll(): Option[List[ByteString]] = {
    if (buffer.nonEmpty) {
      val line = parseLine()
      if (line.nonEmpty) {
        dropReadBuffer()
      }
      line
    } else None
  }

  private def dropReadBuffer() = {
    buffer = buffer.drop(pos)
    pos = 0
    fieldStart = 0
  }

  protected def parseLine(): Option[List[ByteString]] = {
    val buf = buffer
    var columns: Vector[ByteString] = Vector()
    var field: ByteStringBuilder = null
    var fieldCollect = false
    var state: State = LineStart

    def fieldCollector(): Unit = {
      if (!fieldCollect) {
        field = ByteString.newBuilder ++= buf.slice(fieldStart, pos)
        fieldCollect = true
      }
    }

    def fieldAdd(x: Byte): Unit = {
      if (fieldCollect) field += x
    }

    def currentField(pos: Int): ByteString = {
      if (fieldCollect) {
        fieldCollect = false
        field.result()
      } else buf.slice(fieldStart, pos)
    }

    def wrongCharEscaped() = throw new MalformedCSVException(pos, s"wrong escaping at $pos, only escape or delimiter may be escaped")
    def noCharEscaped() = throw new MalformedCSVException(pos, s"wrong escaping at $pos, no character after escape")

    while (state != End && pos < buf.length) {
      val c = buf(pos)
      (state: @switch) match {
        case LineStart => {
          c match {
            case `quoteChar` => {
              state = QuoteStarted
              pos += 1
              fieldStart = pos
            }
            case `delimiter` => {
              columns :+= ByteString.empty
              state = AfterDelimiter
              pos += 1
              fieldStart = pos
            }
            case '\n' | '\u2028' | '\u2029' | '\u0085' => {
              columns :+= ByteString.empty
              state = End
              pos += 1
              fieldStart = pos
            }
            case '\r' => {
              columns :+= ByteString.empty
              state = End
              pos += 1
              if (pos < buf.length && buf(pos) == '\n') {
                pos += 1
              }
              fieldStart = pos
            }
            case x => {
              fieldAdd(x)
              state = WithinField
              pos += 1
            }
          }
        }
        case AfterDelimiter => {
          c match {
            case `quoteChar` => {
              state = QuoteStarted
              pos += 1
              fieldStart = pos
            }
            case `escapeChar` => {
              if (pos + 1 < buf.length
                && (buf(pos + 1) == escapeChar || buf(pos + 1) == delimiter)) {
                fieldCollector()
                fieldAdd(buf(pos + 1))
                state = WithinField
                pos += 2
              } else {
                wrongCharEscaped()
              }
            }
            case `delimiter` => {
              columns :+= ByteString.empty
              state = AfterDelimiter
              pos += 1
              fieldStart = pos
            }
            case '\n' | '\u2028' | '\u2029' | '\u0085' => {
              columns :+= ByteString.empty
              state = End
              pos += 1
              fieldStart = pos
            }
            case '\r' => {
              columns :+= ByteString.empty
              state = End
              pos += 1
              if (pos < buf.length && buf(pos) == '\n') {
                pos += 1
              }
              fieldStart = pos
            }
            case x => {
              fieldAdd(x)
              state = WithinField
              pos += 1
            }
          }
        }
        case WithinField => {
          c match {
            case `escapeChar` => {
              if (pos + 1 < buf.length) {
                if (buf(pos + 1) == escapeChar
                  || buf(pos + 1) == delimiter) {
                  fieldAdd(buf(pos + 1))
                  state = WithinField
                  pos += 2
                } else {
                  wrongCharEscaped()
                }
              } else {
                state = QuoteEnd
                pos += 1
              }
            }
            case `delimiter` => {
              columns :+= currentField(pos)
              state = AfterDelimiter
              pos += 1
              fieldStart = pos
            }
            case '\n' | '\u2028' | '\u2029' | '\u0085' => {
              columns :+= currentField(pos)
              state = End
              pos += 1
              fieldStart = pos
            }
            case '\r' => {
              columns :+= currentField(pos)
              state = End
              pos += 1
              if (pos < buf.length && buf(pos) == '\n') {
                pos += 1
              }
              fieldStart = pos
            }
            case x => {
              fieldAdd(x)
              state = WithinField
              pos += 1
            }
          }
        }
        case QuoteStarted => {
          c match {
            case `escapeChar` if escapeChar != quoteChar => {
              if (pos + 1 < buf.length) {
                if (buf(pos + 1) == escapeChar
                  || buf(pos + 1) == quoteChar) {
                  fieldAdd(buf(pos + 1))
                  state = QuotedField
                  pos += 2
                } else {
                  wrongCharEscaped()
                }
              } else {
                noCharEscaped()
              }
            }
            case `quoteChar` => {
              if (pos + 1 < buf.length && buf(pos + 1) == quoteChar) {
                fieldCollector()
                fieldAdd(c)
                state = QuotedField
                pos += 2
              } else {
                state = QuoteEnd
                pos += 1
              }
            }
            case x => {
              fieldAdd(x)
              state = QuotedField
              pos += 1
            }
          }
        }
        case QuoteEnd => {
          c match {
            case `delimiter` => {
              columns :+= currentField(pos - 1)
              state = AfterDelimiter
              pos += 1
              fieldStart = pos
            }
            case '\n' | '\u2028' | '\u2029' | '\u0085' => {
              columns :+= currentField(pos - 1)
              state = End
              pos += 1
              fieldStart = pos
            }
            case '\r' => {
              columns :+= currentField(pos - 1)
              state = End
              pos += 1
              if (pos < buf.length && buf(pos) == '\n') {
                pos += 1
              }
              fieldStart = pos
            }
            case _ => {
              throw new MalformedCSVException(pos, "expected delimiter or end of line")
            }
          }
        }
        case QuotedField => {
          c match {
            case `escapeChar` if escapeChar != quoteChar => {
              if (pos + 1 < buf.length) {
                if (buf(pos + 1) == escapeChar
                  || buf(pos + 1) == quoteChar) {
                  fieldCollector()
                  fieldAdd(buf(pos + 1))
                  state = QuotedField
                  pos += 2
                } else {
                  wrongCharEscaped()
                }
              } else {
                noCharEscaped()
              }
            }
            case `quoteChar` => {
              if (pos + 1 < buf.length && buf(pos + 1) == quoteChar) {
                fieldCollector()
                fieldAdd(c)
                state = QuotedField
                pos += 2
              } else {
                state = QuoteEnd
                pos += 1
              }
            }
            case x => {
              fieldAdd(x)
              state = QuotedField
              pos += 1
            }
          }
        }
        case End => {
          sys.error("unexpected error")
        }
      }
    }
    (state: @switch) match {
      case AfterDelimiter => {
        columns :+= ByteString.empty
        Some(columns.toList)
      }
      case QuotedField => {
        None
      }
      case _ => {
        state match {
          case WithinField =>
            columns :+= currentField(pos)
          case QuoteEnd => {
            columns :+= currentField(pos - 1)
          }
          case _ => {
          }
        }
        Some(columns.toList)
      }
    }

  }

}
