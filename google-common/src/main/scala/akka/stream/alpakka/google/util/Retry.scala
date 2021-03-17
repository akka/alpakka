/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.util

import akka.actor.Scheduler
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.pattern
import akka.stream.alpakka.google.RetrySettings

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success}

/**
 * A wrapper for a [[Throwable]] indicating that it should be retried.
 */
@InternalApi
private[alpakka] case class Retry private (ex: Throwable) extends Throwable(ex) with NoStackTrace

@InternalApi
private[alpakka] object Retry {

  def apply(ex: Throwable): Retry = ex match {
    case Retry(ex) => new Retry(ex)
    case ex => new Retry(ex)
  }

  /**
   * A wrapper around Akka's [[akka.pattern.RetrySupport]] which requires opt-in.
   * An exception will trigger a retry only if it is wrapped in [[Retry]].
   * Note that the exception will be unwrapped, should all the retry attempts fail
   * (i.e., this method will never raise a [[Retry]], only its underlying exception).
   */
  def apply[T](retrySettings: RetrySettings)(future: => Future[T])(implicit ec: ExecutionContext,
                                                                   scheduler: Scheduler): Future[T] = {
    import retrySettings._
    val futureBuilder = () =>
      future
        .map(Success(_))(ExecutionContexts.parasitic)
        .recover {
          case Retry(ex) => throw ex
          case ex => Failure(ex)
        }(ExecutionContexts.parasitic)
    pattern
      .retry(futureBuilder, maxRetries, minBackoff, maxBackoff, randomFactor)
      .flatMap(Future.fromTry)(ExecutionContexts.parasitic)
  }
}
