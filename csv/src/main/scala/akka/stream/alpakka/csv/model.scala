/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.csv

class MalformedCsvException private[csv] (val lineNo: Long, val bytePos: Int, msg: String) extends Exception(msg) {

  /**
   * Java API:
   * Returns the line number where the parser failed.
   */
  def getLineNo = lineNo

  /**
   * Java API:
   * Returns the byte within the parsed line where the parser failed.
   */
  def getBytePos = bytePos
}
