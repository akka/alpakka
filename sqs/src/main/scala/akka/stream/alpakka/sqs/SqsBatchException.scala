/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.sqs

import akka.annotation.InternalApi

final class SqsBatchException @InternalApi private[sqs] (val batchSize: Int, message: String)
    extends Exception(message) {

  @InternalApi
  private[sqs] def this(batchSize: Int, cause: Throwable) = {
    this(batchSize, cause.getMessage)
    initCause(cause)
  }

  @InternalApi
  private[sqs] def this(batchSize: Int, message: String, cause: Throwable) = {
    this(batchSize, message)
    initCause(cause)
  }

  /** Java API */
  def getBatchSize: Int = batchSize
}
