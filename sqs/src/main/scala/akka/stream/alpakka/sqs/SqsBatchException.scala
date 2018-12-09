/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import akka.annotation.InternalApi

final class SqsBatchException @InternalApi private[sqs] (val batchSize: Int, message: String)
    extends Exception(message) {

  @InternalApi
  private[sqs] def this(batchSize: Int, cause: Throwable) {
    this(batchSize, cause.getMessage)
    initCause(cause)
  }

  /** JAva API */
  def getBatchSize: Int = batchSize
}
