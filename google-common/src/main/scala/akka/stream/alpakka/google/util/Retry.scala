/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.util

import akka.actor.Scheduler
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.pattern
import akka.stream.alpakka.google.RetrySettings
import akka.stream.scaladsl.{Flow, RetryFlow}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.{NoStackTrace, NonFatal}
import scala.util.{Failure, Success, Try}

/**
 * A wrapper for a [[Throwable]] indicating that it should be retried.
 * The underlying exception can be accessed with `getCause()`.
 */
final case class Retry private (ex: Throwable) extends Throwable(ex) with NoStackTrace

object Retry {

  /**
   * Attempts to wrap `ex` as a [[Retry]]. If `ex` is already a [[Retry]] or is fatal then it is not wrapped.
   */
  def apply(ex: Throwable): Throwable = ex match {
    case retry @ Retry(_) => retry
    case NonFatal(ex) => new Retry(ex)
    case ex => ex
  }

  /**
   * Java API: Attempts to wrap `ex` as a [[Retry]]. If `ex` is already a [[Retry]] or is fatal then it is not wrapped.
   */
  def create(ex: Throwable): Throwable = apply(ex)

  /**
   * A wrapper around Akka's [[akka.pattern.RetrySupport]] which requires opt-in.
   * An exception will trigger a retry only if it is wrapped in [[Retry]].
   * Note that the exception will be unwrapped, should all the retry attempts fail
   * (i.e., this method will never raise a [[Retry]], only its underlying exception).
   */
  @InternalApi
  private[alpakka] def apply[T](settings: RetrySettings)(future: => Future[T])(implicit ec: ExecutionContext,
                                                                               scheduler: Scheduler): Future[T] = {
    import settings._
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

  def flow[In, Out, Mat](retrySettings: RetrySettings)(flow: Flow[In, Out, Mat]): Flow[In, Out, Mat] =
    tryFlow[In, Out, Mat](retrySettings)(flow.map(Success(_)).recover {
      case NonFatal(ex) => Failure(ex)
    }).map(_.get)

  def tryFlow[In, Out, Mat](retrySettings: RetrySettings)(flow: Flow[In, Try[Out], Mat]): Flow[In, Try[Out], Mat] = {
    import retrySettings._
    RetryFlow.withBackoff(minBackoff, maxBackoff, randomFactor, maxRetries, flow) {
      case (in, Failure(Retry(_))) => Some(in)
      case _ => None
    }
  }

}
