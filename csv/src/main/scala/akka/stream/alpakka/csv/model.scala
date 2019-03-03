/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
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
