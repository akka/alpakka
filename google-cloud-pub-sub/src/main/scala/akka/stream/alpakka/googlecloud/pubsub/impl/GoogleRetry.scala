/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.impl

import akka.NotUsed
import akka.annotation.InternalApi
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, RetryFlow, Sink, Source}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

/**
 * Applies exponential backoff as specified in the GCS SLA: https://cloud.google.com/storage/sla
 * 1 initial attempt, plus 2^^5 exponential requests to get to 32 seconds
 */
@InternalApi
private[impl] object GoogleRetry {

  def singleRequest(
      http: HttpExt,
      request: HttpRequest
  )(implicit mat: Materializer): Future[HttpResponse] = {
    Source
      .single(request)
      .via(singleRequestFlow(http))
      .runWith(Sink.head)
  }

  def singleRequestFlow(http: HttpExt)(implicit mat: Materializer): Flow[HttpRequest, HttpResponse, NotUsed] = {
    implicit val ec = mat.executionContext
    RetryFlow
      .withBackoff(
        minBackoff = 1.second,
        maxBackoff = 32.seconds,
        randomFactor = 0,
        maxRetries = 6,
        singleRequestResponseFlow(http)
      ) {
        case (_, (req, HttpResponse(StatusCodes.ServerError(_), _, responseEntity, _))) =>
          responseEntity.discardBytes()
          Some(req)
        case _ =>
          None
      }
      .mapAsync(1) {
        case (req, HttpResponse(StatusCodes.ServerError(status), _, responseEntity, _)) =>
          Unmarshal(responseEntity).to[String].map[HttpResponse] { body =>
            throw new RuntimeException(s"Request failed for ${req.method} ${req.uri}, got $status with body: $body")
          }
        case (_, other) => Future.successful(other)
      }
  }

  private def singleRequestResponseFlow(
      http: HttpExt
  )(implicit ec: ExecutionContext): Flow[HttpRequest, (HttpRequest, HttpResponse), NotUsed] = {
    Flow[HttpRequest].mapAsync(1)(req => http.singleRequest(req).map(resp => (req, resp)))
  }
}
