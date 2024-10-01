/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.testkit
import akka.annotation.ApiMayChange
import akka.stream.alpakka.elasticsearch.{ReadResult, WriteMessage, WriteResult}

import scala.jdk.OptionConverters._

object MessageFactory {

  /**
   * Scala API
   * For use with testing.
   */
  @ApiMayChange
  def createReadResult[T](
      id: String,
      source: T,
      version: Option[Long]
  ): ReadResult[T] = new ReadResult(
    id,
    source,
    version
  )

  /**
   * Java API
   * For use with testing.
   */
  @ApiMayChange
  def createReadResult[T](
      id: String,
      source: T,
      version: java.util.Optional[Long]
  ): ReadResult[T] = new ReadResult(
    id,
    source,
    version.toScala
  )
  @ApiMayChange
  def createWriteResult[T, PT](
      message: WriteMessage[T, PT],
      error: Option[String]
  ): WriteResult[T, PT] = new WriteResult(
    message,
    error
  )

  /**
   * Java API
   */
  @ApiMayChange
  def createWriteResult[T, PT](
      message: WriteMessage[T, PT],
      error: java.util.Optional[String]
  ): WriteResult[T, PT] = new WriteResult(
    message,
    error.toScala
  )

}
