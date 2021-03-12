/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.util

import akka.actor.Scheduler
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.util.FastFuture
import akka.pattern
import akka.stream.alpakka.google.RetrySettings

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success}

@InternalApi
private[alpakka] object Retry {

  final case class DoNotRetry(ex: Throwable) extends Throwable with NoStackTrace

  def apply[T](retrySettings: RetrySettings)(future: => Future[T])(implicit ec: ExecutionContext,
                                                                   scheduler: Scheduler): Future[T] = {
    import retrySettings._
    val futureBuilder = () =>
      future
        .map(Success(_))(ExecutionContexts.parasitic)
        .recover {
          case DoNotRetry(ex) => Failure(ex)
        }(ExecutionContexts.parasitic)
    pattern
      .retry(futureBuilder, maxRetries, minBackoff, maxBackoff, randomFactor)
      .flatMap(FastFuture(_))(ExecutionContexts.parasitic)
  }
}
