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
}
