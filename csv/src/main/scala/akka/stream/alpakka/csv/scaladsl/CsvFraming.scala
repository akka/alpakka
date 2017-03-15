/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.csv.scaladsl

import akka.NotUsed
import akka.stream.alpakka.csv.CsvFramingStage
import akka.stream.scaladsl.Flow
import akka.util.ByteString

/** Provides CSV framing stages that can separate CSV lines from incoming [[akka.util.ByteString]] objects. */
object CsvFraming {

  val Backslash: Byte = '\\'
  val Comma: Byte = ','
  val SemiColon: Byte = ';'
  val Colon: Byte = ':'
  val DoubleQuote: Byte = '"'

  def lineScanner(delimiter: Byte = Comma,
                  quoteChar: Byte = DoubleQuote,
                  escapeChar: Byte = Backslash): Flow[ByteString, List[ByteString], NotUsed] =
    Flow[ByteString].via(new CsvFramingStage(delimiter, quoteChar, escapeChar))
}
