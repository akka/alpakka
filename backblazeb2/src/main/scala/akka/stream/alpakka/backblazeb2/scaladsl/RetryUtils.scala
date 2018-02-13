/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2.scaladsl

import akka.stream.alpakka.backblazeb2.Protocol.{B2Error, B2Response}
import cats.syntax.either._

import scala.concurrent.{ExecutionContext, Future, Promise}

object RetryUtils {
  def returnOrObtain[T](promise: Promise[T],
                        f: () => B2Response[T])(implicit executionContext: ExecutionContext): B2Response[T] =
    if (promise.isCompleted) {
      promise.future.map(x => x.asRight[B2Error])
    } else {
      val result = f()

      result map { either => // saving if successful
        either map { x =>
          promise.success(x)
        }
      }

      result
    }

  def tryAgainIfExpired[T](
      x: B2Response[T]
  )(fallbackIfExpired: => B2Response[T])(implicit executionContext: ExecutionContext): Future[Either[B2Error, T]] =
    x flatMap {
      case Left(error) if error.isExpiredToken =>
        fallbackIfExpired

      case Left(error) =>
        Future.successful(error.asLeft)

      case success: Right[B2Error, T] =>
        Future.successful(success)
    }
}
