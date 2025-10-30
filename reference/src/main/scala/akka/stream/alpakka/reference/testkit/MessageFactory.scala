/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.reference.testkit
import akka.annotation.ApiMayChange
import akka.stream.alpakka.reference.{ReferenceReadResult, ReferenceWriteMessage, ReferenceWriteResult}
import akka.util.ByteString

import scala.collection.immutable
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

@ApiMayChange
object MessageFactory {

  @ApiMayChange
  def createReadResult(data: immutable.Seq[ByteString], bytesRead: Try[Int]): ReferenceReadResult =
    new ReferenceReadResult(data, bytesRead)

  /**
   * Java API
   */
  @ApiMayChange
  def createReadResultSuccess(data: java.util.List[ByteString], bytesRead: Int): ReferenceReadResult =
    new ReferenceReadResult(data.asScala.toList, Success(bytesRead))

  /**
   * Java API
   */
  @ApiMayChange
  def createReadResultFailure(data: java.util.List[ByteString], failure: Throwable): ReferenceReadResult =
    new ReferenceReadResult(data.asScala.toList, Failure(failure))

  @ApiMayChange
  def createWriteResult(message: ReferenceWriteMessage, metrics: Map[String, Long], status: Int): ReferenceWriteResult =
    new ReferenceWriteResult(message, metrics, status)

  /**
   * Java API
   */
  @ApiMayChange
  def createWriteResult(message: ReferenceWriteMessage,
                        metrics: java.util.Map[String, java.lang.Long],
                        status: Int): ReferenceWriteResult =
    new ReferenceWriteResult(message, metrics.asScala.iterator.map { case (k, v) => k -> Long.unbox(v) }.toMap, status)

}
