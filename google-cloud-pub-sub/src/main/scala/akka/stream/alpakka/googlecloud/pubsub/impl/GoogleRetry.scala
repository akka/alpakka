/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.impl

import java.util.concurrent.atomic.AtomicInteger

import akka.NotUsed
import akka.annotation.InternalApi
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.pubsub.impl.backport.RetryFlow
import akka.stream.scaladsl.{Flow, RestartSource, Sink, Source}

import scala.concurrent.Future
import scala.concurrent.duration._

@InternalApi
private[googlecloud] object GoogleRetry {

  // Exponential backoff as specified in the GCS SLA: https://cloud.google.com/storage/sla
  // 1 initial attempt, plus 2^5 exponential requests to get to 32 seconds

  def retryingRequestToResponse(
      http: HttpExt,
      request: HttpRequest
  )(implicit mat: Materializer): Future[HttpResponse] = {
    Source
      .single(request)
      .via(singleRequestFlow(http))
      .runWith(Sink.head)
  }

  def singleRequestFlow(http: HttpExt)(implicit mat: Materializer): Flow[HttpRequest, HttpResponse, NotUsed] =
    RetryFlow.withBackoff(
      minBackoff = 1.second,
      maxBackoff = 32.seconds,
      randomFactor = 0,
      maxRetries = 6,
      Flow[HttpRequest].mapAsync(1)(http.singleRequest(_))
    ) { (req, resp) =>
      resp match {
        case HttpResponse(StatusCodes.ServerError(_), _, responseEntity, _) =>
          responseEntity.discardBytes()
          Some(req)
        case _ =>
          None
      }
    }

  def retryingRequestToResponse2(
      http: HttpExt,
      request: HttpRequest
  )(implicit mat: Materializer): Future[HttpResponse] = {
    // Exponential backoff as specified in the GCS SLA: https://cloud.google.com/storage/sla

    // 1 initial attempt, plus 2^5 exponential requests to get to 32 seconds
    val remainingAttempts = new AtomicInteger(6)
    RestartSource
      .withBackoff(
        minBackoff = 1.second,
        maxBackoff = 32.seconds,
        randomFactor = 0
      ) { () =>
        Source
          .fromFuture(http.singleRequest(request))
          // We use mapConcat with an empty output here instead of throwing an exception to restart
          // the Source, as an exception causes stack traces to be logged
          .mapConcat {
            case resp @ HttpResponse(StatusCodes.ServerError(_), _, responseEntity, _) =>
              if (remainingAttempts.getAndDecrement() > 0) {
                responseEntity.discardBytes()
                List()
              } else {
                // We've run out of restarts, so just propagate the error response
                List(resp)
              }
            case other =>
              List(other)
          }
      }
      .runWith(Sink.head)
  }
}
