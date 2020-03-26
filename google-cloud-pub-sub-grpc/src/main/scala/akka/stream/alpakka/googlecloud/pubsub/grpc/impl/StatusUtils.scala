/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.grpc.impl

import akka.annotation.InternalApi
import io.grpc.{Status, StatusRuntimeException}

/**
 * Internal API
 */
@InternalApi private[grpc] object StatusUtils {
  def isRetryable(t: Throwable): Boolean = {
    t match {
      case statusRuntimeException: StatusRuntimeException =>
        statusRuntimeException.getStatus.getCode match {
          case Status.Code.DEADLINE_EXCEEDED | Status.Code.INTERNAL | Status.Code.CANCELLED |
              Status.Code.RESOURCE_EXHAUSTED =>
            true
          case Status.Code.UNAVAILABLE =>
            !statusRuntimeException.getMessage.contains("Server shutdownNow invoked")
          case _ => false
        }
      case _ => false
    }

  }

}
