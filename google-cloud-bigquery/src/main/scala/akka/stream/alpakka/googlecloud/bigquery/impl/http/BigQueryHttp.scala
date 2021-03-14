/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.http

import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.model.headers.Authorization
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.unmarshalling.{FromResponseUnmarshaller, Unmarshal}
import akka.http.scaladsl.util.FastFuture
import akka.http.scaladsl.{Http, HttpExt}
import akka.stream.alpakka.googlecloud.bigquery.impl.util.Retry
import akka.stream.alpakka.googlecloud.bigquery.impl.util.Retry.DoNotRetry
import akka.stream.alpakka.googlecloud.bigquery.{BigQuerySettings, RetrySettings}

import scala.annotation.switch
import scala.concurrent.Future

@InternalApi
private[bigquery] object BigQueryHttp {

  def apply()(implicit system: ClassicActorSystemProvider): BigQueryHttp =
    new BigQueryHttp(Http()(system.classicSystem))

  def apply(http: HttpExt): BigQueryHttp = new BigQueryHttp(http)

  private val standardParamsQuery = "prettyPrint=false"
  private val andStandardParamsQuery = "&" + standardParamsQuery
}

@InternalApi
private[bigquery] final class BigQueryHttp private (val http: HttpExt) extends AnyVal {

  import BigQueryHttp._

  private implicit def system = http.system
  private implicit def ec = system.dispatcher
  private implicit def scheduler = system.scheduler

  def singleRequest(request: HttpRequest)(implicit settings: BigQuerySettings): Future[HttpResponse] = {
    val requestWithStandardParams = request.withUri(
      request.uri.copy(
        rawQueryString = Some(request.uri.rawQueryString.fold(standardParamsQuery)(_.concat(andStandardParamsQuery)))
      )
    )
    settings.forwardProxy.fold(http.singleRequest(requestWithStandardParams)) { proxy =>
      http.singleRequest(requestWithStandardParams, proxy.connectionContext, proxy.poolSettings)
    }
  }

  def singleRequestOrFail(request: HttpRequest)(
      implicit settings: BigQuerySettings,
      exceptionUnmarshaller: FromResponseUnmarshaller[Exception]
  ): Future[HttpResponse] =
    singleRequest(request).flatMap(processResponse)

  def singleRequestWithOAuth(request: HttpRequest)(
      implicit settings: BigQuerySettings
  ): Future[HttpResponse] =
    addOAuth(request).flatMap(singleRequest)(ExecutionContexts.parasitic)

  def singleRequestWithOAuthOrFail(request: HttpRequest)(
      implicit settings: BigQuerySettings,
      exceptionUnmarshaller: FromResponseUnmarshaller[Exception]
  ): Future[HttpResponse] =
    addOAuth(request).flatMap(singleRequestOrFail)(ExecutionContexts.parasitic)

  def retryRequest(request: HttpRequest)(
      implicit settings: BigQuerySettings,
      exceptionUnmarshaller: FromResponseUnmarshaller[Exception]
  ): Future[HttpResponse] = retry(settings.retrySettings)(singleRequest(request))

  def retryRequestWithOAuth(request: HttpRequest)(
      implicit settings: BigQuerySettings,
      exceptionUnmarshaller: FromResponseUnmarshaller[Exception]
  ): Future[HttpResponse] =
    retry(settings.retrySettings) {
      addOAuth(request)
        .transform(identity, DoNotRetry)(ExecutionContexts.parasitic)
        .flatMap(singleRequest)(ExecutionContexts.parasitic)
    }

  private[http] def addOAuth(request: HttpRequest)(implicit settings: BigQuerySettings): Future[HttpRequest] = {
    settings.credentialsProvider
      .getToken()
      .map { token =>
        request.addHeader(Authorization(token))
      }(ExecutionContexts.parasitic)
  }

  private def retry(retrySettings: RetrySettings)(request: => Future[HttpResponse])(
      implicit exceptionUnmarshaller: FromResponseUnmarshaller[Exception]
  ): Future[HttpResponse] =
    Retry(retrySettings) {
      request.flatMap { response =>
        processResponse(response).transform(
          identity,
          ex => {
            (response.status.intValue: @switch) match {
              // Retriable, see https://cloud.google.com/bigquery/docs/error-messages
              // Unfortunately 403 encompasses many errors besides rateLimitExceeded
              case 403 | 500 | 502 | 503 => ex
              case _ => DoNotRetry(ex)
            }
          }
        )(ExecutionContexts.parasitic)
      }(ExecutionContexts.parasitic)
    }

  private def processResponse(
      response: HttpResponse
  )(implicit unmarshaller: FromResponseUnmarshaller[Exception]): Future[HttpResponse] =
    if (response.status.isFailure)
      Unmarshal(response).to[Exception].flatMap(FastFuture.failed)(ExecutionContexts.parasitic)
    else
      FastFuture.successful(response)
}
